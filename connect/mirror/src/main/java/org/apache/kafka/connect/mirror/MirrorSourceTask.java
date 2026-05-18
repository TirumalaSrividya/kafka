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
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.mirror.MirrorConnectorConfig.METRIC_NAMES_LEGACY;
import static org.apache.kafka.connect.mirror.MirrorConnectorConfig.METRIC_NAMES_NEW;

/** Replicates a set of topic-partitions. */
public class MirrorSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(MirrorSourceTask.class);
    // Using Consumer interface instead of KafkaConsumer for testability, flexibilty, loose coupling
    // testability (supports MockConsumer / mocking)
    // flexibility (can plug different Consumer implementations)
    // loose coupling (avoids dependency on concrete KafkaConsumer)
    private Consumer<byte[], byte[]> consumer;
    private String sourceClusterAlias; 
    private Duration pollTimeout;
    private ReplicationPolicy replicationPolicy;
    private MirrorSourceLegacyMetrics legacyMetrics;
    private MirrorSourceMetrics metrics;
    private boolean stopping = false;
    private Semaphore consumerAccess;
    private OffsetSyncWriter offsetSyncWriter;

    // Track expected offsets to detect log truncation and topic resets.
    private final Map<TopicPartition, Long> expectedOffsets = new HashMap<>();
    private final Set<String> compactedTopics = new java.util.HashSet<>();

    public MirrorSourceTask() {}

    // for testing
    MirrorSourceTask(Consumer<byte[], byte[]> consumer, MirrorSourceLegacyMetrics metrics,
                     String sourceClusterAlias, ReplicationPolicy replicationPolicy,
                     OffsetSyncWriter offsetSyncWriter) {
        this.consumer = consumer;
        this.legacyMetrics = metrics;
        this.sourceClusterAlias = sourceClusterAlias;
        this.replicationPolicy = replicationPolicy;
        consumerAccess = new Semaphore(1);
        this.offsetSyncWriter = offsetSyncWriter;
    }

    @Override
    public void start(Map<String, String> props) {
        MirrorSourceTaskConfig config = new MirrorSourceTaskConfig(props);
        consumerAccess = new Semaphore(1);
        sourceClusterAlias = config.sourceClusterAlias();
        List<String> metricNamesFormats = config.metricNamesFormats();
        legacyMetrics = metricNamesFormats.contains(METRIC_NAMES_LEGACY) ? config.legacyMetrics() : null;
        metrics = metricNamesFormats.contains(METRIC_NAMES_NEW) ? config.metrics(context.pluginMetrics()) : null;
        pollTimeout = config.consumerPollTimeout();
        replicationPolicy = config.replicationPolicy();
        if (config.emitOffsetSyncsEnabled()) {
            offsetSyncWriter = new OffsetSyncWriter(config);
        }
        consumer = MirrorUtils.newConsumer(config.sourceConsumerConfig("replication-consumer"));
        Set<TopicPartition> taskTopicPartitions = config.taskTopicPartitions();
        initializeConsumer(taskTopicPartitions);

        log.info("{} replicating {} topic-partitions {}->{}: {}.", Thread.currentThread().getName(),
            taskTopicPartitions.size(), sourceClusterAlias, config.targetClusterAlias(), taskTopicPartitions);
    }

    @Override
    public void commit() {
         // Handle delayed and pending offset syncs only when offsetSyncWriter is available
        if (offsetSyncWriter != null) {
            // Offset syncs which were not emitted immediately due to their offset spacing should be sent periodically
            // This ensures that low-volume topics aren't left with persistent lag at the end of the topic
            offsetSyncWriter.promoteDelayedOffsetSyncs();
            // Publish any offset syncs that we've queued up, but have not yet been able to publish
            // (likely because we previously reached our limit for number of outstanding syncs)
            offsetSyncWriter.firePendingOffsetSyncs();
        }
    }

    @Override
    public void stop() {
        long start = System.currentTimeMillis();
        stopping = true;
        consumer.wakeup();
        try {
            consumerAccess.acquire();
        } catch (InterruptedException e) {
            log.warn("Interrupted waiting for access to consumer. Will try closing anyway.");
        }
        Utils.closeQuietly(consumer, "source consumer");
        Utils.closeQuietly(offsetSyncWriter, "offset sync writer");
        Utils.closeQuietly(legacyMetrics, "metrics");
        log.info("Stopping {} took {} ms.", Thread.currentThread().getName(), System.currentTimeMillis() - start);
    }

    @Override
    public String version() {
        return new MirrorSourceConnector().version();
    }

    @Override
    public List<SourceRecord> poll() {
        if (!consumerAccess.tryAcquire()) {
            return null;
        }
        if (stopping) {
            return null;
        }
        try {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeout);
            List<SourceRecord> sourceRecords = new ArrayList<>(records.count());
            for (ConsumerRecord<byte[], byte[]> record : records) {
                // Topic Partition handles to track offset correctness per partition (Kafka guarantees ordering only within a partition)

                TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                // Ensures replication correctness by detecting offset gaps, resets, or data loss per partition
                if (checkOffsetAnomaly(tp, record.offset())) {
                    break;
                }

                SourceRecord converted = convertRecord(record);
                sourceRecords.add(converted);
                TopicPartition topicPartition = new TopicPartition(converted.topic(), converted.kafkaPartition());
                long age = System.currentTimeMillis() - record.timestamp();
                long size = byteSize(record.value());
                
                if (legacyMetrics != null) {
                    legacyMetrics.recordAge(topicPartition, age);
                    legacyMetrics.recordBytes(topicPartition, size);
                } 
                
                if (metrics != null) {
                    metrics.recordAge(topicPartition, age);
                    metrics.recordBytes(topicPartition, size);
                }
            }
            if (sourceRecords.isEmpty()) {
                // WorkerSourceTasks expects non-zero batch size
                return null;
            } else {
                log.trace("Polled {} records from {}.", sourceRecords.size(), records.partitions());
                return sourceRecords;
            }
        // This block handles the case where the consumer’s offset is outside Kafka’s valid range 
        // delegates recovery or failure decisions to handleOffsetOutOfRange
        // safely ends the current poll cycle.
        } catch (OffsetOutOfRangeException e) {
            handleOffsetOutOfRange(e);
            return null;
        } catch (WakeupException e) {
            return null;
        } catch (KafkaException e) {
            log.warn("Failure during poll.", e);
            return null;
        } catch (Throwable e) {
            log.error("Failure during poll.", e);
            // allow Connect to deal with the exception
            throw e;
        } finally {
            consumerAccess.release();
        }
    }
    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
        if (stopping) {
            return;
        }
        if (metadata == null) {
            log.debug("No RecordMetadata (record filtered during transformation) "
                + "-- can't sync offsets for {}.", record.topic());
            return;
        }
        if (!metadata.hasOffset()) {
            log.error("RecordMetadata has no offset -- can't sync offsets for {}.", record.topic());
            return;
        }
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
        long latency = System.currentTimeMillis() - record.timestamp();
        metrics.countRecord(topicPartition);
        metrics.replicationLatency(topicPartition, latency);
        // Queue offset syncs only when offsetWriter is available
        if (offsetSyncWriter != null) {
            TopicPartition sourceTopicPartition = MirrorUtils.unwrapPartition(record.sourcePartition());
            long upstreamOffset = MirrorUtils.unwrapOffset(record.sourceOffset());
            long downstreamOffset = metadata.offset();
            offsetSyncWriter.maybeQueueOffsetSyncs(sourceTopicPartition, upstreamOffset, downstreamOffset);
            // We may be able to immediately publish an offset sync that we've queued up here
            offsetSyncWriter.firePendingOffsetSyncs();
            
        }
    }

    private Map<TopicPartition, Long> loadOffsets(Set<TopicPartition> topicPartitions) {
        return topicPartitions.stream().collect(Collectors.toMap(x -> x, this::loadOffset));
    }

    private Long loadOffset(TopicPartition topicPartition) {
        Map<String, Object> wrappedPartition = MirrorUtils.wrapPartition(topicPartition, sourceClusterAlias);
        Map<String, Object> wrappedOffset = context.offsetStorageReader().offset(wrappedPartition);
        return MirrorUtils.unwrapOffset(wrappedOffset);
    }

    // visible for testing
    void initializeConsumer(Set<TopicPartition> taskTopicPartitions) {
        Map<TopicPartition, Long> topicPartitionOffsets = loadOffsets(taskTopicPartitions);
        consumer.assign(topicPartitionOffsets.keySet());
        log.info("Starting with {} previously uncommitted partitions.",
            topicPartitionOffsets.values().stream().filter(this::isUncommitted).count());

        topicPartitionOffsets.forEach((topicPartition, offset) -> {
            if (isUncommitted(offset)) {
                log.trace("Skipping seek for uncommitted partition: {}", topicPartition);
                return;
            }
            long nextOffset = offset + 1L;
            log.trace("Seeking to offset {} for topicPartition: {}", nextOffset, topicPartition);
            consumer.seek(topicPartition, nextOffset);
            expectedOffsets.put(topicPartition, nextOffset);
        });

        // Check for data loss at startup — broker may have purged data since last commit.
        Set<TopicPartition> seededPartitions = topicPartitionOffsets.entrySet().stream()
            .filter(e -> !isUncommitted(e.getValue()))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());

        // At startup, validate that required replication data is still available in Kafka.
        // This compares the last committed offset with the earliest available offset in the log.
        //
        // If earliest > committed + 1, it indicates that Kafka has removed unreplicated data,
        // typically due to retention policies (time or size-based deletion).
        //
        // Log-compacted topics are excluded because compaction removes older records
        // while retaining the latest value per key, making such gaps expected and non-critical.
        
        if (!seededPartitions.isEmpty()) {
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(seededPartitions);
            for (Map.Entry<TopicPartition, Long> entry : beginningOffsets.entrySet()) {
                TopicPartition tp = entry.getKey();
                long earliest = entry.getValue();
                long committed = topicPartitionOffsets.get(tp);
                if (earliest > committed + 1) {
                    if (compactedTopics.contains(tp.topic())) {
                        log.debug("Startup offset gap on compacted topic {} — not data loss.", tp);
                        continue;
                    }
                    log.error("DATA LOSS DETECTED on {} at startup. "
                        + "Committed offset: {}, Earliest available: {}.", tp, committed, earliest);
                    throw new DataLossException("Data loss detected at startup on " + tp);
                }
            }
        }
    }

    // visible for testing
    SourceRecord convertRecord(ConsumerRecord<byte[], byte[]> record) {
        String targetTopic = formatRemoteTopic(record.topic());
        Headers headers = convertHeaders(record);
        return new SourceRecord(
            MirrorUtils.wrapPartition(new TopicPartition(record.topic(), record.partition()), sourceClusterAlias),
            MirrorUtils.wrapOffset(record.offset()),
            targetTopic, record.partition(),
            Schema.OPTIONAL_BYTES_SCHEMA, record.key(),
            Schema.BYTES_SCHEMA, record.value(),
            record.timestamp(), headers);
    }

    private Headers convertHeaders(ConsumerRecord<byte[], byte[]> record) {
        ConnectHeaders headers = new ConnectHeaders();
        for (Header header : record.headers()) {
            headers.addBytes(header.key(), header.value());
        }
        return headers;
    }

    private String formatRemoteTopic(String topic) {
        return replicationPolicy.formatRemoteTopic(sourceClusterAlias, topic);
    }

    private static int byteSize(byte[] bytes) {
        if (bytes == null) {
            return 0;
        } else {
            return bytes.length;
        }
    }

    private boolean isUncommitted(Long offset) {
        return offset == null || offset < 0;
    }

    private boolean checkOffsetAnomaly(TopicPartition tp, long incoming) {
        if (!expectedOffsets.containsKey(tp)) {
            log.info("First record seen for {}. Baselining expected offset at {}.", tp, incoming);
            expectedOffsets.put(tp, incoming + 1L);
            return false;
        }
        long expected = expectedOffsets.get(tp);
        if (isDataLoss(incoming, expected)) {
            if (compactedTopics.contains(tp.topic())) {
                log.debug("Offset gap on compacted topic {} (expected={}, got={}). "
                    + "Normal compaction — advancing expected offset.", tp, expected, incoming);
                expectedOffsets.put(tp, incoming + 1L);
                return false;
            }
            log.error("DATA LOSS DETECTED on {}! Expected offset {}, got {}. "
                + "Data purged by retention before replication.", tp, expected, incoming);
            throw new DataLossException("Data loss detected on " + tp);
        }
        if (isTopicReset(incoming, expected)) {
            log.error("TOPIC RESET DETECTED on {}! Expected offset {}, got {}. "
                + "Timestamp: {}. Topic was likely deleted and recreated.",
                tp, expected, incoming, new Date());
            log.info("Seeking {} to beginning for automatic recovery.", tp);
            consumer.seekToBeginning(Collections.singletonList(tp));
            expectedOffsets.put(tp, 0L);
            return true;
        }
        expectedOffsets.put(tp, incoming + 1L);
        return false;
    }

    private void handleOffsetOutOfRange(OffsetOutOfRangeException e) {
        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(
            e.offsetOutOfRangePartitions().keySet());

        for (TopicPartition tp : e.offsetOutOfRangePartitions().keySet()) {
            Long expected  = expectedOffsets.get(tp);
            Long beginning = beginningOffsets.get(tp);

            // Case A — partition never tracked; no basis to seek anywhere
            if (expected == null) {
                log.warn("OffsetOutOfRange on untracked partition {}. "
                    + "No expected offset recorded — skipping.", tp);
                continue;
            }

            // Case B — beginningOffsets returned nothing; cannot decide
            if (beginning == null) {
                log.warn("OffsetOutOfRange on {} but beginningOffsets returned null. "
                    + "Cannot determine cause — letting Connect retry.", tp);
                continue;
            }

            // Confirmed data loss: broker log start moved past our expected offset
            if (expected < beginning) {
                log.error("DATA LOSS DETECTED: Log truncation on {}. Expected: {}, Earliest: {}",
                    tp, expected, beginning);
                throw new DataLossException("Log truncation detected on " + tp);
            }

            // Confirmed topic reset: topic deleted and recreated (beginning reset to 0)
            if (beginning == 0 && expected > 0) {
                log.warn("TOPIC RESET DETECTED (OffsetOutOfRange) on {}. "
                    + "Expected offset {}. Timestamp: {}. Seeking to beginning.",
                    tp, expected, new Date());
                consumer.seekToBeginning(Collections.singletonList(tp));
                expectedOffsets.put(tp, 0L);
                continue;
            }

            // Case C — ambiguous (transient error or misconfiguration); do not seek
            log.warn("Ambiguous OffsetOutOfRange on {}. Expected: {}, Beginning: {}. "
                + "Not seeking — will retry on next poll.", tp, expected, beginning);
        }
    }

    // Returns true if incoming offset jumped forward — data purged before replication.
    private boolean isDataLoss(long incoming, long expected) {
        return incoming > expected;
    }

    // Returns true if incoming offset went backward — topic deleted and recreated.
    private boolean isTopicReset(long incoming, long expected) {
        return incoming < expected;
    }

    public void putExpectedOffset(TopicPartition tp, long offset) {
        expectedOffsets.put(tp, offset);
    }

    public void markTopicAsCompacted(String topic) {
        compactedTopics.add(topic);
    }

    public static class DataLossException extends RuntimeException {
        public DataLossException(String message) {
            super(message);
        }
    }
}
