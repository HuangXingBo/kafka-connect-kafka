package com.connector.tool;

import com.connector.source.kafka.ConsumerUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class SourceConnectorTestTool {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        String sourceBootServers = args[0];
        props.put("bootstrap.servers", sourceBootServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        String topicName = args[1];
        int msgNum = 200000;

        System.out.println("source bootstrap.servers " + sourceBootServers);
        System.out.println("source topic " + args[1]);
        String targetBootServers = args[2];
        String targetTopic = args[3];
        System.out.println("target bootstrap.servers " + targetBootServers);
        System.out.println("target topic " + targetTopic);
        Producer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(props);
        for (int i = 0; i < msgNum; i++) {
            producer.send(new ProducerRecord<byte[], byte[]>(topicName, String.valueOf(i).getBytes()));
            if (i % 1000 == 0) {
                producer.flush();
                System.out.println(i);
                Thread.sleep(1000);
            }
        }
        System.out.println("total count " + msgNum);
        producer.close();

        Thread.sleep(5000);
        System.out.println("start verify");

        Properties consumerProp = ConsumerUtil.createDefaultKafkaConsumerProperties("source_connector_", Arrays.asList(targetBootServers));
        consumerProp.put("auto.offset.reset", "earliest");
        Consumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(consumerProp);
        consumer.subscribe(Collections.singletonList(targetTopic));

        Thread.sleep(5000);
        int[] bucket = new int[msgNum];
        int count = 0;
        ConsumerRecords<byte[], byte[]> records = consumer.poll(1000);
        for (int i = 0; i < 10; i++) {
            if (records.isEmpty()) {
                records = consumer.poll(1000);
            } else {
                break;
            }
        }
        while (!records.isEmpty()) {
            for (ConsumerRecord<byte[], byte[]> record : records) {
                bucket[Integer.parseInt(new String(record.value()))]++;
                count++;
            }
            records = consumer.poll(1000);
        }
        System.out.println("source topic message count = " + msgNum);
        System.out.println("target topic message count = " + count);

        if (msgNum != count) {
            System.out.println("duplicated or deficiency key are :");
            for (int i = 0; i < msgNum; i++) {
                if (bucket[i] != 1) {
                    System.out.println("key = " + i + " count = " + bucket[i]);
                }
            }
        }
    }
}
