package com.connector.source.kafka;

import com.connector.Version;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class KafkaSourceConnector extends SourceConnector {

    public static final String CONNECTOR_NAME = "name";

    public KafkaSourceConnectorConfig connectorConfig;

    //the name of the connector
    private String connectorName;
    //the consumer is be used to get the infos of topic partitions.
    //we will use admin client from the version of 0.11 in kafka
    private KafkaConsumer consumer;


    @Override
    public void start(Map<String, String> props) {
        connectorName = props.get(CONNECTOR_NAME);
        connectorConfig = new KafkaSourceConnectorConfig(props);
        consumer = new KafkaConsumer(ConsumerUtil.createDefaultKafkaConsumerProperties(connectorName,
                connectorConfig.getList(KafkaSourceConnectorConfig.SOURCE_BOOTSTRAP_SERVERS_CONFIG)));
    }

    @Override
    public Class<? extends Task> taskClass() {
        return KafkaSourceTask.class;
    }

    /**
     * create configs for tasks to execute.
     *
     * @param maxTasks
     * @return
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        Pattern topicPattern = connectorConfig.getTopicPattern();
        List<String> topics = regexFilterTopic(topicPattern);
        String[] topicPartitionsToTask = splitTopicPartitionToTask(topics, maxTasks);
        List<Map<String, String>> configs = new ArrayList<>(topicPartitionsToTask.length);
        for (int i = 0; i < topicPartitionsToTask.length; i++) {
            Map<String, String> taskConfig = new HashMap<>();
            taskConfig.putAll(connectorConfig.allAsStrings());
            taskConfig.put(CONNECTOR_NAME, connectorName);
            taskConfig.put(KafkaSourceConnectorConfig.TOPIC_PARTITIONS, topicPartitionsToTask[i]);
            configs.add(taskConfig);
        }
        return configs;
    }

    /**
     * regex pattern to filter topic
     *
     * @param topicPattern
     * @return
     */
    private List<String> regexFilterTopic(Pattern topicPattern) {
        List<String> filterTopic = new LinkedList<>();
        Map<String, List<PartitionInfo>> topics = consumer.listTopics();
        String topic;
        for (Map.Entry<String, List<PartitionInfo>> entry : topics.entrySet()) {
            topic = entry.getKey();
            if (topicPattern.matcher(entry.getKey()).matches()) {
                filterTopic.add(topic);
            }
        }
        return filterTopic;
    }

    /**
     * split topic-partition to task, the strategy is round-robin
     *
     * @param topics
     * @param taskNum
     * @return
     */
    private String[] splitTopicPartitionToTask(List<String> topics, int taskNum) {
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        for (String topic : topics) {
            partitionsPerTopic.put(topic, consumer.partitionsFor(topic).size());
        }
        Map<Integer, List<TopicPartition>> assignments = ConsumerUtil.assign(partitionsPerTopic, taskNum);
        String[] result = ConsumerUtil.serializeToString(assignments);
        return result;
    }

    @Override
    public void stop() {
        consumer.close();
    }

    @Override
    public ConfigDef config() {
        return KafkaSourceConnectorConfig.CONFIG;
    }

    @Override
    public String version() {
        return Version.version();
    }
}
