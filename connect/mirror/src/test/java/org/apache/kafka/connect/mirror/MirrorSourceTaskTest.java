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
import java.util.HashSet;
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
        assertEquals("cluster7.topic1", sourceRecord.topic(),"Failure on cluster7.topic1 consumerRecord serde");
        assertEquals(2, sourceRecord.kafkaPartition().intValue(),"sourceRecord kafka partition is incorrect");
        assertEquals(new TopicPartition("topic1", 2),
                MirrorUtils.unwrapPartition(sourceRecord.sourcePartition()),"topic1 unwrapped from sourcePartition is incorrect");
        assertEquals(3L, MirrorUtils.unwrapOffset(sourceRecord.sourceOffset()).longValue(),"sourceRecord's sourceOffset is incorrect");
        assertEquals(4L, sourceRecord.timestamp().longValue(),"sourceRecord's timestamp is incorrect");
        assertEquals(key, sourceRecord.key(),"sourceRecord's key is incorrect");
        assertEquals(value, sourceRecord.value(),"sourceRecord's value is incorrect");
        assertEquals(headers.lastHeader("header1").value(),
                sourceRecord.headers().lastWithName("header1").value(),"sourceRecord's header1 is incorrect"));
        assertEquals(headers.lastHeader("header2").value(),
                sourceRecord.headers().lastWithName("header2").value(),"sourceRecord's header2 is incorrect");
    }

    @Test
    public void testOffsetSync() {
        OffsetSyncWriter.PartitionState partitionState = new OffsetSyncWriter.PartitionState(50);

        assertTrue(partitionState.update(0, 100), "always emit offset sync on first update");
        assertTrue(partitionState.shouldSyncOffsets, "should sync offsets");
        partitionState.reset();
        assertFalse(partitionState.shouldSyncOffsets, "should sync offsets to false");
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
        assertFalse(partitionState.shouldSyncOffsets, "should sync offsets to false");
    }

    @Test
    public void testZeroOffsetSync() {
        OffsetSyncWriter.PartitionState partitionState = new OffsetSyncWriter.PartitionState(0);

        // if max offset lag is zero, should always emit offset syncs
        assertTrue(partitionState.update(0, 100), "zeroOffsetSync downStreamOffset 100 is incorrect");
        assertTrue(partitionState.shouldSyncOffsets, "should sync offsets"));
        partitionState.reset();
        assertFalse(partitionState.shouldSyncOffsets, "should sync offsets to false");
        assertTrue(partitionState.update(2, 102), "zeroOffsetSync downStreamOffset 102 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(3, 153), "zeroOffsetSync downStreamOffset 153 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(4, 154), "zeroOffsetSync downStreamOffset 154 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(5, 155), "zeroOffsetSync downStreamOffset 155 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(6, 207), "zeroOffsetSync downStreamOffset 207 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(2, 208), "zeroOffsetSync downStreamOffset 208 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(3, 209), "zeroOffsetSync downStreamOffset 209 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(4, 3), "zeroOffsetSync downStreamOffset 3 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(5, 4), "zeroOffsetSync downStreamOffset 4 is incorrect");
        assertTrue(partitionState.update(7, 6), "zeroOffsetSync downStreamOffset 6 is incorrect");
        assertTrue(partitionState.update(7, 6), "zeroOffsetSync downStreamOffset 6 is incorrect");
        assertTrue(partitionState.update(8, 7), "zeroOffsetSync downStreamOffset 7 is incorrect");
        assertTrue(partitionState.update(10, 57), "zeroOffsetSync downStreamOffset 57 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(11, 58), "zeroOffsetSync downStreamOffset 58 is incorrect");
    }

    @Test
    public void testPoll() {
        byte[] key1 = "abc".getBytes();
        byte[] value1 = "fgh".getBytes();
        byte[] key2 = "123".getBytes();
        byte[] value2 = "456".getBytes();
        List<ConsumerRecord<byte[], byte[]>> consumerRecordsList = new ArrayList<>();
        String topicName = "test";
        String headerkey = "key";
        RecordHeaders headers = new RecordHeaders(new Header[] {
            new RecordHeader(headerkey, "value".getBytes()),
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

        String sourceClusterName = "cluster1";
        ReplicationPolicy replicationPolicy = new DefaultReplicationPolicy();
        MirrorSourceTask mirrorSourceTask = new MirrorSourceTask(consumer, metrics, sourceClusterName,
                replicationPolicy, null);
        List<SourceRecord> sourceRecords = mirrorSourceTask.poll();

        assertEquals(2, sourceRecords.size());
        for (int i = 0; i < sourceRecords.size(); i++) {
            SourceRecord sourceRecord = sourceRecords.get(i);
            ConsumerRecord<byte[], byte[]> consumerRecord = consumerRecordsList.get(i);
            assertEquals(consumerRecord.key(), sourceRecord.key(), "consumerRecord key does not equal sourceRecord key");
            assertEquals(consumerRecord.value(), sourceRecord.value(), "consumerRecord value does not equal sourceRecord value");
              // We expect that the topicname will be based on the replication policy currently used
            assertEquals(replicationPolicy.formatRemoteTopic("cluster1", topicName), sourceRecord.topic());
            // We expect that MirrorMaker will keep the same partition assignment
            assertEquals(consumerRecord.partition(), sourceRecord.kafkaPartition().intValue(), "partition assignment not the same as the current replicationPolicy");
             // Check header values
            List<Header> expectedHeaders = new ArrayList<>();
            consumerRecord.headers().forEach(expectedHeaders::add);
            List<org.apache.kafka.connect.header.Header> taskHeaders = new ArrayList<>();
            sourceRecord.headers().forEach(taskHeaders::add);
            compareHeaders(expectedHeaders, taskHeaders);
        }
    }

    @Test
    public void testSeekBehaviorDuringStart() {
        // Setting up mock behaviour.
        @SuppressWarnings("unchecked")
        Consumer<byte[], byte[]> mockConsumer = mock(Consumer.class);

        SourceTaskContext mockSourceTaskContext = mock(SourceTaskContext.class);
        OffsetStorageReader mockOffsetStorageReader = mock(OffsetStorageReader.class);
        when(mockSourceTaskContext.offsetStorageReader()).thenReturn(mockOffsetStorageReader);

        Set<TopicPartition> topicPartitions = new HashSet<>(Arrays.asList(
                new TopicPartition("previouslyReplicatedTopic", 8),
                new TopicPartition("previouslyReplicatedTopic1", 0),
                new TopicPartition("previouslyReplicatedTopic", 1),
                new TopicPartition("newTopicToReplicate1", 1),
                new TopicPartition("newTopicToReplicate1", 4),
                new TopicPartition("newTopicToReplicate2", 0)));

        long arbitraryCommittedOffset = 4L;
        long offsetToSeek = arbitraryCommittedOffset + 1L;
        when(mockOffsetStorageReader.offset(anyMap())).thenAnswer(testInvocation -> {
            Map<String, Object> topicPartitionOffsetMap = testInvocation.getArgument(0);
            String topicName = topicPartitionOffsetMap.get("topic").toString();

            // Only return the offset for previously replicated topics.
            // For others, there is no value set.
            if (topicName.startsWith("previouslyReplicatedTopic")) {
                topicPartitionOffsetMap.put("offset", arbitraryCommittedOffset);
            }
            return topicPartitionOffsetMap;
        });

        when(mockConsumer.beginningOffsets(any())).thenReturn(Collections.emptyMap());

        MirrorSourceTask mirrorSourceTask = new MirrorSourceTask(
                mockConsumer, null, null, new DefaultReplicationPolicy(), null);
        mirrorSourceTask.initialize(mockSourceTaskContext);

        // Call test subject
        mirrorSourceTask.initializeConsumer(topicPartitions);

        // Verifications
        // Ensure all the topic partitions are assigned to consumer
        verify(mockConsumer, times(1)).assign(topicPartitions);

        // Ensure seek is only called for previously committed topic partitions.
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
        // Create a consumer mock
        byte[] key1 = "abc".getBytes();
        byte[] value1 = "fgh".getBytes();
        String topicName = "test";
        String headerKey = "key";
        RecordHeaders headers = new RecordHeaders(new Header[] {
            new RecordHeader(headerKey, "value".getBytes()),
        });

        @SuppressWarnings("unchecked")
        Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        MirrorSourceLegacyMetrics metrics = mock(MirrorSourceLegacyMetrics.class);
        
        String sourceClusterName = "cluster1";
        ReplicationPolicy replicationPolicy = new DefaultReplicationPolicy();
        MirrorSourceTask mirrorSourceTask = new MirrorSourceTask(consumer, metrics, sourceClusterName,
                new DefaultReplicationPolicy(), null);

        SourceRecord sourceRecord = mirrorSourceTask.convertRecord(
                new ConsumerRecord<>("test", 0, 0, System.currentTimeMillis(),
                        TimestampType.CREATE_TIME, key1.length, value1.length, key1, value1, headers, Optional.empty()));

        //Expect that commitRecord will not throw an exception
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

        RecordHeaders headers = new RecordHeaders();
        ReplicationPolicy replicationPolicy = new DefaultReplicationPolicy();

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
        // We should have dispatched this sync to the producer
        verify(offsetSyncWriter, times(1)).maybeQueueOffsetSyncs(
                eq(sourceTopicPartition), eq((long) recordOffset), eq(recordMetadata.offset()));
        verify(offsetSyncWriter, times(1)).firePendingOffsetSyncs();

        mirrorSourceTask.commit();
        // No more syncs should take place; we've been able to publish all of them so far
        verify(offsetSyncWriter, times(1)).promoteDelayedOffsetSyncs();
        verify(offsetSyncWriter, times(2)).firePendingOffsetSyncs();
    }

    // initializeConsumer detects dataloss at startup
    // if earlierst available offset is ahead of committed offset, the data is purged before replication indicates a DatalossException must be thrown
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

        // Calling the REAL initializeConsumer() — this throws DataLossException
        // because earliest (200) > committed + 1 (101).
        assertThrows(DataLossException.class,
                () -> task.initializeConsumer(Collections.singleton(tp)),
                "DataLossException must be thrown when broker earliest offset "
                        + "is ahead of committed offset");
    }


    // Verifies that records collected from other partitions are not discarded, when a topic reset is detected mid-batch on one partition.
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

    // Verifies that offset gaps on compacted topics are not treated as data loss since compaction deletes records causing gaps in offsets.
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

    // Verifies that out-of-range offsets throw a DataLossException
    @Test
    public void testOffsetOutOfRangeThrowsDataLossException() {
        TopicPartition tp = new TopicPartition("test-topic", 0);

        MockConsumer<byte[], byte[]> mockConsumer = new MockConsumer<>(OffsetResetStrategy.NONE);
        // Assign the consumer to a single partition "test-topic-0"
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

    //Verifies that OffsetOutOfRangeException on an untracked partition (no expected offset recorded), do not trigger a seek or DataLossException
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

    // When a topic is deleted and recreated, broker resets to offset 0.
    // Verifies that consumer seeks to beginning to resume replication from scratch.
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
            org.apache.kafka.connect.header.Header taskHeader = taskHeaders.get(i);
            assertEquals(expected.key(), taskHeader.key(), "taskHeader's key expected to equal " + taskHeader.key());
            assertEquals(expected.value(), taskHeader.value(), "taskHeader's value expected to equal " + taskHeader.value().toString());
        }
    }
}
