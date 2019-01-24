# kafka-connect-kafka
A Kafka Source Connector for [Kafka Connect](https://kafka.apache.org/documentation/#connect). Mirror topics from a source Kafka cluster into your destination Kafka cluster.

## Build

The kafka-kafka source connector is based on 0.10.2 version of Kafka

Output a fat jar to `target/kafkaConnect-1.0-SNAPSHOT.jar` by running:

`mvn clean install`

Copy the resulting jar file into your Kafka libs.

## Configuration Options

Note that these options are for this MirrorTool connector, and assumes some familiarity with the base Kafka Connect configuration options. (You will need to configure the parameters for your destination Kafka Cluster in the base Kafka Connect configuration)

## Common Options

These are the most common options that are required when configuring this connector:

Configuration Parameter | Example | Description
----------------------- | ------- | -----------
**source.bootstrap.servers** | source.broker1:9092,source.broker2:9092 | **Mandatory.** Comma separated list of boostrap servers for the source Kafka cluster
**topic** | topic, topic-prefix* | Java regular expression to match topics to mirror. For convenience, comma (',') is interpreted as the regex-choice symbol ('|').'
**connector.consumer.group.id** | custom-kafka-source-test-consumer | Group ID used when writing offsets back to source cluster (for offset lag tracking)
**connector.consumer.client.id** | custom-kafka-source-test-client | Client ID used for controling the speed of consuming
**topic.mapping** | source_topic1=target_topic;source_topic2=target_topic2 | it is used for renaming or copying from multi source topics to a target topic.Every pair is split by Semi-colon (';').source topic and target topic are split by equal sign('=')

## Advanced Options

Some use cases may require modifying the following default connector options. Use with care.

Configuration Parameter | Default | Description
----------------------- | ------- | -----------
**poll.loop.timeout.ms** | 1000 | Maximum amount of time (in milliseconds) the connector will wait in each poll loop without data before returning control to the kafka connect task thread.
**connector.consumer.max.shutdown.wait.ms** | 2000 | Maximum amount of time (in milliseconds) to wait for the connector to gracefully shut down before forcing the consumer and admin clients to close. Note that any values greater than the kafka connect parameter *task.shutdown.graceful.timeout.ms* will not have any effect.
**connector.consumer.max.poll.records** | 500 | Maximum number of records to return from each poll of the internal KafkaConsumer. When dealing with topics with very large messages, the connector may sometimes spend too long processing each batch of records, causing lag in offset commits, or in serious cases, unnecessary consumer rebalances. Reducing this value can help in these scenarios. Conversely, when processing very small messages, increasing this value may improve overall throughput.
**connector.consumer.key.deserializer** | org.apache.kafka.common.serialization.ByteArrayDeserializer | Key deserializer to use for the kafka consumers connecting to the source cluster.
**connector.consumer.value.deserializer** | org.apache.kafka.common.serialization.ByteArrayDeserializer | Value deserializer to use for the kafka consumers connecting to the source cluster.
Note that these offsets are not used to resume the connector (They are stored in the Kafka Connect offset store), but may be useful in monitoring the current offset lag of this connector on the source cluster

### Overriding the internal KafkaConsumer Configuration

For cases where the configuration for the KafkaConsumer, you can use the more explicit "*connector.consumer.*" configuration parameter prefixes to fine tune the settings used for each.

### Example Configuration

```javascript
{
	"name":"custom-kafka-source-test", // Name of the kafka connect task
	"config":{
		"connector.class":"com.connector.source.kafka.KafkaSourceConnector",//choose source connector path
		"tasks.max":"4", // Maximum number of parallel tasks to run
		"source.bootstrap.servers":"localhost:9092",// Kafka boostrap servers for source cluster
		"connector.consumer.group.id":"custom-kafka-source-test-consumer",//consumer group id
		"connector.consumer.client.id":"kafka-connect-testing-client",//consumer client id
		"connector.consumer.partition.assignment.strategy":"org.apache.kafka.clients.consumer.RoundRobinAssignor",//partitioner
		"topic":".*connector_test",// Mirror topics matching this regex
		"topic.mapping":"source_connector_test=source_connector_test_map" // Topic mapping
	}
}
```
