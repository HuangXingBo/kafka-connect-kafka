package com.connector.source.kafka;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.CircularIterator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConsumerUtil {

    /**
     * create the default consumer
     * <p>
     * group.id = ${connector_name} + _consumer
     * client.id = ${connector_name} + _client
     *
     * @param name
     * @param bootServer
     * @return
     */
    public static Properties createDefaultKafkaConsumerProperties(String name, List<String> bootServer) {
        Properties consumerConfigProps = new Properties();
        consumerConfigProps.put("enable.auto.commit", "false");
        consumerConfigProps.setProperty("key.deserializer", ByteArrayDeserializer.class.getName());
        consumerConfigProps.setProperty("value.deserializer", ByteArrayDeserializer.class.getName());
        consumerConfigProps.put("group.id", name + "_consumer");
        consumerConfigProps.put("client.id", name + "_client");
        consumerConfigProps.put("bootstrap.servers", String.join(",", bootServer));
        consumerConfigProps.put("max.poll.records", "500");
        consumerConfigProps.put("auto.offset.reset", "latest");
        return consumerConfigProps;
    }

    /**
     * assign topic-partition to task, the strategy is round-robin.
     *
     * @param partitionsPerTopic
     * @param taskNum
     * @return
     */
    public static Map<Integer, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic, int taskNum) {
        int partitionCount = 0;
        for (int num : partitionsPerTopic.values()) {
            partitionCount += num;
        }
        taskNum = Math.min(partitionCount, taskNum);
        Map<Integer, List<TopicPartition>> assignment = new HashMap<>(taskNum);
        for (int i = 0; i < taskNum; i++) {
            assignment.put(i, new ArrayList<>());
        }
        CircularIterator<Integer> assigner = new CircularIterator<>(new ArrayList<>(assignment.keySet()));
        List<TopicPartition> allTopicPartitions = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : partitionsPerTopic.entrySet()) {
            String topic = entry.getKey();
            int partitionNum = entry.getValue();
            for (int i = 0; i < partitionNum; i++) {
                allTopicPartitions.add(new TopicPartition(topic, i));
            }
        }

        for (TopicPartition partition : allTopicPartitions) {
            assignment.get(assigner.next()).add(partition);
        }
        return assignment;
    }

    /**
     * serialize the assignments to string for the use of task configs
     *
     * @param assignments
     * @return
     */
    public static String[] serializeToString(Map<Integer, List<TopicPartition>> assignments) {
        String[] result = new String[assignments.size()];
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < result.length; i++) {
            List<TopicPartition> topicPartitions = assignments.get(i);
            sb.append(intToString(topicPartitions.size()));
            for (TopicPartition tp : topicPartitions) {
                String topic = tp.topic();
                int partitionNum = tp.partition();
                sb.append(intToString(topic.length()));
                sb.append(topic);
                sb.append(intToString(partitionNum));
            }
            result[i] = sb.toString();
            sb.delete(0, sb.length());
        }
        return result;
    }

    /**
     * deserialize the string of assignment to List<TopicPartition> which will be used in source task.
     *
     * @param assignment
     * @return
     */
    public static List<TopicPartition> deserializeString(String assignment) {
        List<TopicPartition> topicPartitions = new ArrayList<>();
        int len;
        int parLen;
        int index = 0;
        byte[] byteArr = assignment.getBytes();
        len = stringToInt(byteArr, index);
        index += 8;
        for (int j = 0; j < len; j++) {
            parLen = stringToInt(byteArr, index);
            index += 8;
            String topic = assignment.substring(index, index + parLen);
            index += parLen;
            parLen = stringToInt(byteArr, index);
            index += 8;
            TopicPartition tp = new TopicPartition(topic, parLen);
            topicPartitions.add(tp);
        }

        return topicPartitions;
    }

    /**
     * int to String
     * e.g 100 -> 00000100
     *
     * @param src
     * @return
     */
    private static String intToString(int src) {
        return String.format("%08d", src);
    }

    /**
     * String to int
     *
     * @param src
     * @param index
     * @return
     */
    private static int stringToInt(byte[] src, int index) {
        int value = 0;
        for (int i = index, end = index + 8; i < end; i++) {
            value = value * 10 + (src[i] - 48);
        }
        return value;
    }

    @Deprecated
    private static byte[] intToBytes(int src) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(src);
        byte[] target = new byte[4];
        buffer.rewind();
        buffer.get(target);
        buffer.clear();
        return target;
    }

    @Deprecated
    private static int bytesToInt(byte[] src, int index) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.put(src, index, 4);
        buffer.rewind();
        int target = buffer.getInt();
        buffer.clear();
        return target;
    }


    public static void main(String[] args) {
    }
}
