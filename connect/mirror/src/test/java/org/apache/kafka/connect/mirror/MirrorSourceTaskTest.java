/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.mirror.MirrorSourceTask.DataLossException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class MirrorSourceTaskTest {

    // =========================================================================
    // Existing tests — unchanged from original, kept for regression coverage
    // =========================================================================

    @Test
    public void testSerde() {
        byte[] key = new byte[] {'a', 'b', 'c', 'd', 'e'};
        byte[] value = new byte[] {'f', 'g', 'h', 'i', 'j', 'k'};
        Headers headers = new RecordHeaders();
        headers.add("header1", new byte[] {'l', 'm', 'n', 'o'});
        headers.add("header2", new byte[] {'p', 'q', 'r', 's', 't'});
        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>(
                "topic1", 2, 3L, 4L,
                TimestampType.CREATE_TIME, 5, 6, key, value, headers, Optional.empty());
        MirrorSourceTask mirrorSourceTask = new MirrorSourceTask(
                null, null, "cluster7", new DefaultReplicationPolicy(), null);
        SourceRecord sourceRecord = mirrorSourceTask.convertRecord(consumerRecord);
        assertEquals("cluster7.topic1", sourceRecord.topic());
        assertEquals(2, sourceRecord.kafkaPartition().intValue());
        assertEquals(new TopicPartition("topic1", 2),
                MirrorUtils.unwrapPartition(sourceRecord.sourcePartition()));
        assertEquals(3L, MirrorUtils.unwrapOffset(sourceRecord.sourceOffset()).longValue());
        assertEquals(4L, sourceRecord.timestamp().longValue());
        assertEquals(key, sourceRecord.key());
        assertEquals(value, sourceRecord.value());
        assertEquals(headers.lastHeader("header1").value(),
                sourceRecord.headers().lastWithName("header1").value());
        assertEquals(headers.lastHeader("header2").value(),
                sourceRecord.headers().lastWithName("header2").value());
    }

    @Test
    public void testOffsetSync() {
        OffsetSyncWriter.PartitionState partitionState = new OffsetSyncWriter.PartitionState(50);
        assertTrue(partitionState.update(0, 100), "always emit offset sync on first update");
        assertTrue(partitionState.shouldSyncOffsets);
        partitionState.reset();
        assertFalse(partitionState.shouldSyncOffsets);
        assertTrue(partitionState.update(2, 102), "upstream offset skipped -> resync");
        partitionState.reset();
        assertFalse(partitionState.update(3, 152), "no sync");
        partitionState.reset();
        assertTrue(partitionState.update(4, 153), "one past target offset");
        partitionState.reset();
        assertFalse(partitionState.update(5, 154), "no sync");
        partitionState.reset();
        assertFalse(partitionState.update(6, 203), "no sync");
        partitionState.reset();
        assertTrue(partitionState.update(7, 204), "one past target offset");
        partitionState.reset();
        assertTrue(partitionState.update(2, 206), "upstream reset");
        partitionState.reset();
        assertFalse(partitionState.update(3, 207), "no sync");
        partitionState.reset();
        assertTrue(partitionState.update(4, 3), "downstream reset");
        partitionState.reset();
        assertFalse(partitionState.update(5, 4), "no sync");
        assertTrue(partitionState.update(7, 6), "sync");
        assertTrue(partitionState.update(7, 6), "sync");
        assertTrue(partitionState.update(8, 7), "sync");
        assertTrue(partitionState.update(10, 57), "sync");
        partitionState.reset();
        assertFalse(partitionState.update(11, 58), "sync");
        assertFalse(partitionState.shouldSyncOffsets);
    }

    @Test
    public void testZeroOffsetSync() {
        OffsetSyncWriter.PartitionState partitionState = new OffsetSyncWriter.PartitionState(0);
        assertTrue(partitionState.update(0, 100));
        assertTrue(partitionState.shouldSyncOffsets);
        partitionState.reset();
        assertFalse(partitionState.shouldSyncOffsets);
        assertTrue(partitionState.update(2, 102));
        partitionState.reset();
        assertTrue(partitionState.update(3, 153));
        partitionState.reset();
        assertTrue(partitionState.update(4, 154));
        partitionState.reset();
        assertTrue(partitionState.update(5, 155));
        partitionState.reset();
        assertTrue(partitionState.update(6, 207));
        partitionState.reset();
        assertTrue(partitionState.update(2, 208));
        partitionState.reset();
        assertTrue(partitionState.update(3, 209));
        partitionState.reset();
        assertTrue(partitionState.update(4, 3));
        partitionState.reset();
        assertTrue(partitionState.update(5, 4));
        assertTrue(partitionState.update(7, 6));
        assertTrue(partitionState.update(7, 6));
        assertTrue(partitionState.update(8, 7));
        assertTrue(partitionState.update(10, 57));
        partitionState.reset();
        assertTrue(partitionState.update(11, 58));
    }

    @Test
    public void testPoll() {
        byte[] key1 = "abc".getBytes();
        byte[] value1 = "fgh".getBytes();
        byte[] key2 = "123".getBytes();
        byte[] value2 = "456".getBytes();
        List<ConsumerRecord<byte[], byte[]>> consumerRecordsList = new ArrayList<>();
        String topicName = "test";
        RecordHeaders headers = new RecordHeaders(new Header[] {
            new RecordHeader("key", "value".getBytes()),
        });
        consumerRecordsList.add(new ConsumerRecord<>(topicName, 0, 0,
                System.currentTimeMillis(), TimestampType.CREATE_TIME,
                key1.length, value1.length, key1, value1, headers, Optional.empty()));
        consumerRecordsList.add(new ConsumerRecord<>(topicName, 1, 1,
                System.currentTimeMillis(), TimestampType.CREATE_TIME,
                key2.length, value2.length, key2, value2, headers, Optional.empty()));

        final TopicPartition tp = new TopicPartition(topicName, 0);
        ConsumerRecords<byte[], byte[]> consumerRecords = new ConsumerRecords<>(
                Map.of(tp, consumerRecordsList),
                Map.of(tp, new OffsetAndMetadata(2, Optional.empty(), "")));

        @SuppressWarnings("unchecked")
        Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        when(consumer.poll(any())).thenReturn(consumerRecords);

        MirrorSourceLegacyMetrics metrics = mock(MirrorSourceLegacyMetrics.class);
        ReplicationPolicy replicationPolicy = new DefaultReplicationPolicy();
        MirrorSourceTask mirrorSourceTask = new MirrorSourceTask(
                consumer, metrics, "cluster1", replicationPolicy, null);

        List<SourceRecord> sourceRecords = mirrorSourceTask.poll();
        assertEquals(2, sourceRecords.size());
        for (int i = 0; i < sourceRecords.size(); i++) {
            SourceRecord sr = sourceRecords.get(i);
            ConsumerRecord<byte[], byte[]> cr = consumerRecordsList.get(i);
            assertEquals(cr.key(), sr.key());
            assertEquals(cr.value(), sr.value());
            assertEquals(replicationPolicy.formatRemoteTopic("cluster1", topicName), sr.topic());
            assertEquals(cr.partition(), sr.kafkaPartition().intValue());
            List<Header> expectedHeaders = new ArrayList<>();
            cr.headers().forEach(expectedHeaders::add);
            List<org.apache.kafka.connect.header.Header> taskHeaders = new ArrayList<>();
            sr.headers().forEach(taskHeaders::add);
            compareHeaders(expectedHeaders, taskHeaders);
        }
    }

    @Test
    public void testSeekBehaviorDuringStart() {
        @SuppressWarnings("unchecked")
        Consumer<byte[], byte[]> mockConsumer = mock(Consumer.class);

        SourceTaskContext mockSourceTaskContext = mock(SourceTaskContext.class);
        OffsetStorageReader mockOffsetStorageReader = mock(OffsetStorageReader.class);
        when(mockSourceTaskContext.offsetStorageReader()).thenReturn(mockOffsetStorageReader);

        Set<TopicPartition> topicPartitions = Set.of(
                new TopicPartition("previouslyReplicatedTopic", 8),
                new TopicPartition("previouslyReplicatedTopic1", 0),
                new TopicPartition("previouslyReplicatedTopic", 1),
                new TopicPartition("newTopicToReplicate1", 1),
                new TopicPartition("newTopicToReplicate1", 4),
                new TopicPartition("newTopicToReplicate2", 0));

        long arbitraryCommittedOffset = 4L;
        long offsetToSeek = arbitraryCommittedOffset + 1L;

        when(mockOffsetStorageReader.offset(anyMap())).thenAnswer(inv -> {
            Map<String, Object> m = inv.getArgument(0);
            String topicName = m.get("topic").toString();
            if (topicName.startsWith("previouslyReplicatedTopic")) {
                m.put("offset", arbitraryCommittedOffset);
            }
            return m;
        });

        when(mockConsumer.beginningOffsets(any())).thenReturn(Collections.emptyMap());

        MirrorSourceTask mirrorSourceTask = new MirrorSourceTask(
                mockConsumer, null, null, new DefaultReplicationPolicy(), null);
        mirrorSourceTask.initialize(mockSourceTaskContext);

        mirrorSourceTask.initializeConsumer(topicPartitions);

        verify(mockConsumer, times(1)).assign(topicPartitions);
        verify(mockConsumer, times(1))
                .seek(new TopicPartition("previouslyReplicatedTopic", 8), offsetToSeek);
        verify(mockConsumer, times(1))
                .seek(new TopicPartition("previouslyReplicatedTopic", 1), offsetToSeek);
        verify(mockConsumer, times(1))
                .seek(new TopicPartition("previouslyReplicatedTopic1", 0), offsetToSeek);
        // beginningOffsets is now called for the startup data-loss check
        verify(mockConsumer, times(1)).beginningOffsets(any());

        verifyNoMoreInteractions(mockConsumer);
    }

    @Test
    public void testCommitRecordWithNullMetadata() {
        @SuppressWarnings("unchecked")
        Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        MirrorSourceLegacyMetrics metrics = mock(MirrorSourceLegacyMetrics.class);
        MirrorSourceTask mirrorSourceTask = new MirrorSourceTask(
                consumer, metrics, "cluster1", new DefaultReplicationPolicy(), null);

        RecordHeaders headers = new RecordHeaders(new Header[] {
            new RecordHeader("key", "value".getBytes()),
        });
        SourceRecord sourceRecord = mirrorSourceTask.convertRecord(
                new ConsumerRecord<>("test", 0, 0, System.currentTimeMillis(),
                        TimestampType.CREATE_TIME, 3, 3,
                        "abc".getBytes(), "fgh".getBytes(), headers, Optional.empty()));

        // Must not throw
        mirrorSourceTask.commitRecord(sourceRecord, null);
    }

    @Test
    public void testSendSyncEvent() {
        long maxOffsetLag = 50;
        int recordPartition = 0;
        int recordOffset = 0;
        int metadataOffset = 100;
        String topicName = "topic";
        String sourceClusterName = "sourceCluster";

        @SuppressWarnings("unchecked")
        Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        MirrorSourceLegacyMetrics metrics = mock(MirrorSourceLegacyMetrics.class);
        OffsetSyncWriter offsetSyncWriter = mock(OffsetSyncWriter.class);
        when(offsetSyncWriter.maxOffsetLag()).thenReturn(maxOffsetLag);
        doNothing().when(offsetSyncWriter).firePendingOffsetSyncs();
        doNothing().when(offsetSyncWriter).promoteDelayedOffsetSyncs();

        MirrorSourceTask mirrorSourceTask = new MirrorSourceTask(
                consumer, metrics, sourceClusterName, new DefaultReplicationPolicy(), offsetSyncWriter);

        SourceRecord sourceRecord = mirrorSourceTask.convertRecord(
                new ConsumerRecord<>(topicName, recordPartition, recordOffset,
                        System.currentTimeMillis(), TimestampType.CREATE_TIME,
                        3, 5, "key".getBytes(), "value".getBytes(),
                        new RecordHeaders(), Optional.empty()));

        TopicPartition sourceTopicPartition = MirrorUtils.unwrapPartition(sourceRecord.sourcePartition());
        RecordMetadata recordMetadata = new RecordMetadata(
                sourceTopicPartition, metadataOffset, 0, 0, 0, recordPartition);
        doNothing().when(offsetSyncWriter).maybeQueueOffsetSyncs(
                eq(sourceTopicPartition), eq((long) recordOffset), eq(recordMetadata.offset()));

        mirrorSourceTask.commitRecord(sourceRecord, recordMetadata);
        verify(offsetSyncWriter, times(1)).maybeQueueOffsetSyncs(
                eq(sourceTopicPartition), eq((long) recordOffset), eq(recordMetadata.offset()));
        verify(offsetSyncWriter, times(1)).firePendingOffsetSyncs();

        mirrorSourceTask.commit();
        verify(offsetSyncWriter, times(1)).promoteDelayedOffsetSyncs();
        verify(offsetSyncWriter, times(2)).firePendingOffsetSyncs();
    }

    @Test
    public void testDataLossDetectedAtStartup() {
        TopicPartition tp = new TopicPartition("test-topic", 0);

        // committed offset = 100 → nextOffset to seek = 101
        // beginningOffsets returns 200 → 200 > 101 → data loss must be thrown
        @SuppressWarnings("unchecked")
        Consumer<byte[], byte[]> mockConsumer = mock(Consumer.class);
        doNothing().when(mockConsumer).assign(any());
        doNothing().when(mockConsumer).seek(any(), anyLong());
        when(mockConsumer.beginningOffsets(any()))
                .thenReturn(Collections.singletonMap(tp, 200L));

        SourceTaskContext mockContext = mock(SourceTaskContext.class);
        OffsetStorageReader mockReader = mock(OffsetStorageReader.class);
        when(mockContext.offsetStorageReader()).thenReturn(mockReader);
        when(mockReader.offset(anyMap())).thenAnswer(inv -> {
            // Return committed offset 100 for the test partition
            Map<String, Object> m = new HashMap<>(inv.getArgument(0));
            m.put("offset", 100L);
            return m;
        });

        MirrorSourceTask task = new MirrorSourceTask(
                mockConsumer, null, "primary", new DefaultReplicationPolicy(), null);
        task.initialize(mockContext);

        // Calling the REAL initializeConsumer() — this must throw DataLossException
        // because earliest (200) > committed + 1 (101).
        assertThrows(DataLossException.class,
                () -> task.initializeConsumer(Collections.singleton(tp)),
                "DataLossException must be thrown when broker earliest offset "
                        + "is ahead of committed offset");
    }

    @Test
    public void testTopicResetDoesNotDropOtherPartitions() {
        TopicPartition tp0 = new TopicPartition("test-topic", 0);
        TopicPartition tp1 = new TopicPartition("test-topic", 1);

        MockConsumer<byte[], byte[]> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        mockConsumer.assign(Arrays.asList(tp0, tp1));
        mockConsumer.updateBeginningOffsets(Map.of(tp0, 0L, tp1, 0L));
        mockConsumer.seek(tp0, 50L);
        mockConsumer.seek(tp1, 50L);

        MirrorSourceTask task = new MirrorSourceTask(
                mockConsumer, null, "primary", new DefaultReplicationPolicy(), null);

        // tp1 at expected offset 50 → normal record that should be returned
        task.putExpectedOffset(tp1, 50L);
        // tp0 at expected offset 50, incoming offset 0 → topic reset
        task.putExpectedOffset(tp0, 50L);

        // BUG 4 FIX verification:
        // Add tp1 record first (offset 50 = normal), then tp0 record (offset 0 = reset).
        // With the fix (break instead of return null), the tp1 record must be returned.
        mockConsumer.addRecord(new ConsumerRecord<>(
                "test-topic", 1, 50L, null, "good-msg".getBytes()));
        mockConsumer.addRecord(new ConsumerRecord<>(
                "test-topic", 0, 0L, null, "reset-msg".getBytes()));

        List<SourceRecord> result = task.poll();

        // The good record from tp1 must be returned — not silently discarded.
        assertNotNull(result, "poll() should return the record collected before the reset");
        assertEquals(1, result.size(),
                "Exactly 1 record (from tp1) should be returned before loop break");

        // Both partitions must still be assigned after the reset on tp0.
        Set<TopicPartition> assigned = mockConsumer.assignment();
        assertTrue(assigned.contains(tp0));
        assertTrue(assigned.contains(tp1));
    }

    @Test
    public void testCompactedTopicOffsetGapIsNotDataLoss() {
        TopicPartition tp = new TopicPartition("compacted-topic", 0);

        MockConsumer<byte[], byte[]> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        mockConsumer.assign(Collections.singletonList(tp));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));

        // Mark topic as compacted so gaps are ignored
        MirrorSourceTask task = new MirrorSourceTask(
                mockConsumer, null, "primary", new DefaultReplicationPolicy(), null);
        task.markTopicAsCompacted("compacted-topic");

        // Baseline: expected next offset is 5
        task.putExpectedOffset(tp, 5L);

        // Add a record at offset 10 — gap of 5 (compaction deleted 5,6,7,8,9)
        mockConsumer.addRecord(new ConsumerRecord<>(
                "compacted-topic", 0, 10L, null, "compacted-msg".getBytes()));

        // Must NOT throw DataLossException — gap is normal for compacted topic
        List<SourceRecord> result = task.poll();
        assertNotNull(result, "Compacted topic gap must not be treated as data loss");
        assertEquals(1, result.size());
    }

    @Test
    public void testOffsetOutOfRangeThrowsDataLossException() {
        TopicPartition tp = new TopicPartition("test-topic", 0);

        MockConsumer<byte[], byte[]> mockConsumer = new MockConsumer<>(OffsetResetStrategy.NONE);
        mockConsumer.assign(Collections.singletonList(tp));
        // beginning = 300, expected = 100 → expected(100) < beginning(300) → data loss
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(tp, 300L));
        mockConsumer.seek(tp, 100L);
        mockConsumer.schedulePollTask(() -> {
            throw new OffsetOutOfRangeException(Collections.singletonMap(tp, 100L));
        });

        MirrorSourceTask task = new MirrorSourceTask(
                mockConsumer, null, "primary", new DefaultReplicationPolicy(), null);
        task.putExpectedOffset(tp, 100L);

        assertThrows(DataLossException.class, task::poll,
                "Data loss must be detected when expected offset is behind broker beginning");
    }

    @Test
    public void testOffsetOutOfRangeUntrackedPartitionDoesNotSeek() {
        TopicPartition tp = new TopicPartition("test-topic", 0);

        @SuppressWarnings("unchecked")
        Consumer<byte[], byte[]> mockConsumer = mock(Consumer.class);
        // beginning = 50
        when(mockConsumer.beginningOffsets(any()))
                .thenReturn(Collections.singletonMap(tp, 50L));
        when(mockConsumer.poll(any())).thenThrow(
                new OffsetOutOfRangeException(Collections.singletonMap(tp, 10L)));

        MirrorSourceTask task = new MirrorSourceTask(
                mockConsumer, null, "primary", new DefaultReplicationPolicy(), null);
        // Intentionally do NOT call putExpectedOffset → expected == null

        // Must not throw DataLossException and must not call seekToBeginning
        List<SourceRecord> result = task.poll();
        assertNull(result);

        verify(mockConsumer, times(0)).seekToBeginning(any());
    }

    @Test
    public void testOffsetOutOfRangeConfirmedTopicResetSeeksToBeginning() {
        TopicPartition tp = new TopicPartition("test-topic", 0);

        MockConsumer<byte[], byte[]> mockConsumer = new MockConsumer<>(OffsetResetStrategy.NONE);
        mockConsumer.assign(Collections.singletonList(tp));
        // beginning = 0 after topic recreation
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
        mockConsumer.seek(tp, 500L);
        mockConsumer.schedulePollTask(() -> {
            throw new OffsetOutOfRangeException(Collections.singletonMap(tp, 500L));
        });

        MirrorSourceTask task = new MirrorSourceTask(
                mockConsumer, null, "primary", new DefaultReplicationPolicy(), null);
        // expected = 500 > 0 = beginning → confirmed topic reset
        task.putExpectedOffset(tp, 500L);

        // Must not throw — must seek to beginning and return null
        List<SourceRecord> result = task.poll();
        assertNull(result);
        // MockConsumer position should now be 0 (beginning)
        assertEquals(0L, mockConsumer.position(tp));
    }

    private void compareHeaders(List<Header> expectedHeaders,
            List<org.apache.kafka.connect.header.Header> taskHeaders) {
        assertEquals(expectedHeaders.size(), taskHeaders.size());
        for (int i = 0; i < expectedHeaders.size(); i++) {
            Header expected = expectedHeaders.get(i);
            org.apache.kafka.connect.header.Header actual = taskHeaders.get(i);
            assertEquals(expected.key(), actual.key());
            assertEquals(expected.value(), actual.value());
        }
    }
}