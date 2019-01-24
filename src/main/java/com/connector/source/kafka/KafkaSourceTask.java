package com.connector.source.kafka;

import com.connector.Version;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaSourceTask extends SourceTask {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceTask.class);

    private static final String TOPIC_FIELD = "topic";

    private static final String PARTITION_FIELD = "partition";

    private static final String POSITION_FIELD = "position";

    private static final Schema KEY_SCHEMA = Schema.BYTES_SCHEMA;

    private static final Schema VALUE_SCHEMA = Schema.BYTES_SCHEMA;

    // Consumer
    private KafkaConsumer<byte[], byte[]> consumer;

    // Used to ensure we can be nice and call consumer.close() on shutdown
    private final CountDownLatch stopLatch = new CountDownLatch(1);
    // Flag to the poll() loop that we are awaiting shutdown so it can clean up.
    private AtomicBoolean stop = new AtomicBoolean(false);
    // Flag to the stop() function that it needs to wait for poll() to wrap up before trying to close the kafka consumer.
    private AtomicBoolean poll = new AtomicBoolean(false);
    // Used to enforce synchronized access to stop and poll
    private final Object stopLock = new Object();

    //topic partitions assigned
    private List<TopicPartition> topicPartitions;
    //topic mapping
    private Map<String, String> topicMapping;

    //manually commit offsets for watching lag in upstreaming kafka
    Map<TopicPartition, Long> offsets = new HashMap<>();


    // Settings
    private int maxShutdownWait;
    private int pollTimeout;

    @Override
    public String version() {
        return Version.version();
    }

    @Override
    public void start(Map<String, String> opts) {
        KafkaSourceConnectorConfig sourceConnectorConfig = new KafkaSourceConnectorConfig(opts);

        maxShutdownWait = sourceConnectorConfig.getInt(KafkaSourceConnectorConfig.MAX_SHUTDOWN_WAIT_MS_CONFIG);
        pollTimeout = sourceConnectorConfig.getInt(KafkaSourceConnectorConfig.POLL_LOOP_TIMEOUT_MS_CONFIG);
        topicPartitions = ConsumerUtil.deserializeString(opts.get(KafkaSourceConnectorConfig.TOPIC_PARTITIONS));
        topicMapping = parseTopicMapping(sourceConnectorConfig.getString(sourceConnectorConfig.SOURCE_TOPIC_MAPPING_CONFIG));
        consumer = new KafkaConsumer<byte[], byte[]>(sourceConnectorConfig.getKafkaConsumerProperties());
        consumer.assign(topicPartitions);
        seekOffset();
    }

    /**
     * parse mapping str to map
     * e.g. source_topic_0=target_topic_0;source_topic_1=target_topic_1 will be parsed to
     * Map(source_topic_0 -> target_topic_0, source_topic_1 -> target_topic_1)
     *
     * @param mappingStr
     * @return
     */
    private Map<String, String> parseTopicMapping(String mappingStr) {
        Map<String, String> topicMapping = new HashMap<>();
        if (mappingStr != null && !mappingStr.isEmpty()) {
            String[] mapingPairs = mappingStr.split(";");
            for (String pair : mapingPairs) {
                String[] topicMapPair = pair.split("=");
                if (topicMapPair.length != 2) {
                    continue;
                }
                topicMapping.put(topicMapPair[0], topicMapPair[1]);
            }
        }
        return topicMapping;
    }

    /**
     * poll the kafka source records into the local kafka
     *
     * @return
     */
    @Override
    public List<SourceRecord> poll() {
        synchronized (stopLock) {
            if (!stop.get()) {
                poll.set(true);
            }
        }
        List<SourceRecord> result = new ArrayList<>();
        if (poll.get()) {
            try {
                ConsumerRecords<byte[], byte[]> records = receive();
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    String targetTopic = topicMapping.getOrDefault(record.topic(), record.topic());
                    if (record.key() != null) {
                        result.add(new SourceRecord(offsetKey(record), offsetValue(record), targetTopic, null,
                                KEY_SCHEMA, record.key(), VALUE_SCHEMA, record.value()));
                    } else {
                        result.add(new SourceRecord(offsetKey(record), offsetValue(record), targetTopic, null,
                                VALUE_SCHEMA, record.value()));
                    }
                    offsets.put(new TopicPartition(record.topic(), record.partition()), record.offset() + 1);
                }

            } catch (WakeupException e) {
                LOG.info("{}: Caught WakeupException. Probably shutting down.", this);
            }
        }
        maybeCommitOffset();
        poll.set(false);
        if (stop.get()) {
            stopLatch.countDown();
        }
        return result;
    }

    /**
     * batch commit kafka consumer offset in upstreaming.
     */
    private void maybeCommitOffset() {
        Map<TopicPartition, OffsetAndMetadata> commitOffset = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
            commitOffset.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
        }
        try {
            consumer.commitSync(commitOffset);
        } catch (WakeupException e) {
            LOG.info("{}: Consumer commitSync Caught WakeupException. Probably shutting down.", this);
        }
        offsets.clear();
    }

    /**
     * receive kafka source data
     *
     * @return
     */
    private ConsumerRecords<byte[], byte[]> receive() {
        return consumer.poll(pollTimeout);
    }

    /**
     * be sure that the consumer can seek the correct offset when restart or delete/create the same name
     */
    private void seekOffset() {
        OffsetStorageReader reader = context.offsetStorageReader();
        List<Map<String, Object>> topicPartitionDesc = new ArrayList<>();
        for (TopicPartition topicPartition : topicPartitions) {
            Map<String, Object> topicPartitionMap = new HashMap<>();
            topicPartitionMap.put(TOPIC_FIELD, topicPartition.topic());
            topicPartitionMap.put(PARTITION_FIELD, topicPartition.partition());
            topicPartitionDesc.add(topicPartitionMap);
        }
        Map<Map<String, Object>, Map<String, Object>> offsets = reader.offsets(topicPartitionDesc);
        List<TopicPartition> seekEndTopicPartition = new ArrayList<>();
        if (offsets != null) {
            for (Map.Entry<Map<String, Object>, Map<String, Object>> entry : offsets.entrySet()) {
                Map<String, Object> tpMap = entry.getKey();
                TopicPartition tp = new TopicPartition((String) tpMap.get(TOPIC_FIELD), (Integer) tpMap.get(PARTITION_FIELD));
                Map<String, Object> position = entry.getValue();
                if (position != null) {
                    Object lastRecordedOffset = position.get(POSITION_FIELD);
                    if (lastRecordedOffset != null && lastRecordedOffset instanceof Long) {
                        consumer.seek(tp, (Long) lastRecordedOffset);
                    } else {
                        seekEndTopicPartition.add(tp);
                    }
                } else {
                    seekEndTopicPartition.add(tp);
                }
            }
            if (!seekEndTopicPartition.isEmpty()) {
                consumer.seekToEnd(seekEndTopicPartition);
            }
        } else {
            consumer.seekToEnd(topicPartitions);
        }
    }

    /**
     * use the topic-partition pair to save different topic-partition key
     *
     * @param record
     * @return
     */
    private Map<String, Object> offsetKey(ConsumerRecord record) {
        Map<String, Object> offsetKey = new HashMap<>();
        offsetKey.put(TOPIC_FIELD, record.topic());
        offsetKey.put(PARTITION_FIELD, record.partition());
        return offsetKey;
    }

    /**
     * use the map the save the position in the topic-partition
     *
     * @param record
     * @return
     */
    private Map<String, Long> offsetValue(ConsumerRecord record) {
        return Collections.singletonMap(POSITION_FIELD, record.offset() + 1);
    }

    @Override
    public void stop() {
        long startWait = System.currentTimeMillis();
        synchronized (stopLock) {
            stop.set(true);
            consumer.wakeup();
            if (poll.get()) {
                try {
                    stopLatch.await(Math.max(0, maxShutdownWait - (System.currentTimeMillis() - startWait)), TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                }
            }
            consumer.close(Math.max(0, maxShutdownWait - (System.currentTimeMillis() - startWait)), TimeUnit.MILLISECONDS);
        }
    }
}
