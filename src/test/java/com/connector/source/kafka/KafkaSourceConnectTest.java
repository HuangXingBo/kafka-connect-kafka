package com.connector.source.kafka;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaSourceConnectTest {

    private static final String ZKHOST = "127.0.0.1";
    private static final String BROKERHOST = "127.0.0.1";
    private static final String BROKERPORT = "9092";
    private static final String TOPIC1 = "test1";
    private static final String TOPIC2 = "test2";
    private static final String TOPIC3 = "test3";

    private EmbeddedZookeeper zkServer;

    private ZkClient zkClient;

    private ZkUtils zkUtils;

    private KafkaServer kafkaServer;

    @Before
    public void setUp() throws Exception {
        // setup Zookeeper
        zkServer = new EmbeddedZookeeper();
        String zkConnect = ZKHOST + ":" + zkServer.port();
        zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
        zkUtils = ZkUtils.apply(zkClient, false);

        // setup Broker
        Properties brokerProps = new Properties();
        brokerProps.setProperty("zookeeper.connect", zkConnect);
        brokerProps.setProperty("broker.id", "0");
        brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        brokerProps.setProperty("listeners", "PLAINTEXT://" + BROKERHOST + ":" + BROKERPORT);
        brokerProps.setProperty("offsets.topic.replication.factor", "1");
        KafkaConfig config = new KafkaConfig(brokerProps);
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);
    }

    @Test
    public void createTopic() {
        // create topic
        AdminUtils.createTopic(zkUtils, TOPIC1, 3, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
        AdminUtils.createTopic(zkUtils, TOPIC2, 5, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
        AdminUtils.createTopic(zkUtils, TOPIC3, 7, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
//        // setup producer
//        Properties producerProps = new Properties();
//        producerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
//        producerProps.setProperty("key.serializer","org.apache.kafka.common.serialization.IntegerSerializer");
//        producerProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
//        KafkaProducer<Integer, byte[]> producer = new KafkaProducer<Integer, byte[]>(producerProps);

        // setup consumer
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
        consumerProps.setProperty("group.id", "group0");
        consumerProps.setProperty("client.id", "consumer0");
        consumerProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put("auto.offset.reset", "earliest");  // to make sure the consumer starts from the beginning of the topic
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps);
        assert consumer.partitionsFor(TOPIC1).size() == 3;
        assert consumer.partitionsFor(TOPIC2).size() == 5;
        assert consumer.partitionsFor(TOPIC3).size() == 7;

//        // send message
//        ProducerRecord<Integer, byte[]> data = new ProducerRecord<>(TOPIC, 42, "test-message".getBytes(StandardCharsets.UTF_8));
//        producer.send(data);
//        producer.close();
//
//        // starting consumer
//        ConsumerRecords<Integer, byte[]> records = consumer.poll(5000);
//        assertEquals(1, records.count());
//        Iterator<ConsumerRecord<Integer, byte[]>> recordIterator = records.iterator();
//        ConsumerRecord<Integer, byte[]> record = recordIterator.next();
//        System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
//        assertEquals(42, (int) record.key());
//        assertEquals("test-message", new String(record.value(), StandardCharsets.UTF_8));
    }


    @Test
    public void testSplitTopicToTask() {
        // create topic
        AdminUtils.createTopic(zkUtils, TOPIC1, 3, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
        AdminUtils.createTopic(zkUtils, TOPIC2, 5, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
        AdminUtils.createTopic(zkUtils, TOPIC3, 7, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
        Properties prop = ConsumerUtil.createDefaultKafkaConsumerProperties("test", Arrays.asList(BROKERHOST + ":" + BROKERPORT));
        KafkaConsumer consumer = new KafkaConsumer<>(prop);
        String[] topics = new String[]{TOPIC1, TOPIC2, TOPIC3};
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        for (String topic : topics) {
            partitionsPerTopic.put(topic, consumer.partitionsFor(topic).size());
        }
        Map<Integer, List<TopicPartition>> assignments = ConsumerUtil.assign(partitionsPerTopic, 4);
        String[] result = ConsumerUtil.serializeToString(assignments);
        Map<Integer, List<TopicPartition>> assignmentsDes = new HashMap<>();
        for (int i = 0; i < assignments.size(); i++) {
            assignmentsDes.put(i, ConsumerUtil.deserializeString(result[i]));
        }
        assert assignments.equals(assignmentsDes);
    }

    @Test
    public void testConnectorAndTaskConfig() {
        KafkaSourceConnector connector = new KafkaSourceConnector();
        Map<String, String> props = new HashMap<>();
        props.put("name", "custom-kafka-source");
        props.put("connector.class", "com.kafka.learn.connector.KafkaSourceConnector");
        props.put("tasks.max", "40");
        props.put("source.bootstrap.servers", "b1.global.kafka.dev.aws.fwmrm.net:9092");
        props.put("connector.consumer.group.id", "custom-kafka-source_consumer");
        props.put("connector.consumer.client.id", "custom-kafka-source_client");
        props.put("topic", "fw_ads_binary_log_prod");
        props.put("topic.mapping", "fw_ads_binary_log_prod=fw_ads_binary_log_prod_map");
        connector.start(props);
        List<Map<String, String>> result = connector.taskConfigs(4);

        KafkaSourceConnectorConfig sourceConnectorConfig = new KafkaSourceConnectorConfig(result.get(0));
        Properties consumerProperties = sourceConnectorConfig.getKafkaConsumerProperties();
        Properties defaultProperties = ConsumerUtil.createDefaultKafkaConsumerProperties("custom-kafka-source", Arrays.asList("b1.global.kafka.dev.aws.fwmrm.net:9092"));
        assert consumerProperties.equals(defaultProperties);
    }

    @After
    public void shutdown() {
        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();

    }
}
