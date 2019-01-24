package com.connector.source.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class KafkaSourceConnectorConfig extends AbstractConfig {

    private static final Validator NON_EMPTY_LIST_VALIDATOR = new ConfigDef.Validator() {

        @Override
        public void ensureValid(String name, Object value) {
            if (((List<String>) value).isEmpty()) {
                throw new ConfigException("At least one bootstrap server must be configured in " + name);
            }
        }
    };

    private static final Validator NON_EMPTY_STRING_VALIDATOR = new ConfigDef.Validator() {

        @Override
        public void ensureValid(String name, Object o) {
            String s = (String) o;
            if (s != null && s.isEmpty()) {
                throw new ConfigException(name, o, "String must be non-empty");
            }
        }

        @Override
        public String toString() {
            return "non-empty string";
        }
    };

    private static final Validator TOPIC_WHITELIST_REGEX_VALIDATOR = new ConfigDef.Validator() {

        @Override
        public void ensureValid(String name, Object value) {
            getTopicWhitelistPattern((String) value);
        }
    };

    // Config Prefixes
    public static final String SOURCE_PREFIX = "source.";
    public static final String DESTINATION_PREFIX = "destination.";

    // Any CONFIG beginning with this prefix will set the CONFIG parameters for the kafka consumer used in this connector
    public static final String CONSUMER_PREFIX = "connector.consumer.";

    public static final String TASK_PREFIX = "task.";


    // General Connector CONFIG
    //TOPICS
    public static String SOURCE_TOPIC_WHITELIST_CONFIG = "topic";
    public static final String SOURCE_TOPIC_WHITELIST_DOC = "Regular expressions indicating the topics to consume from the source cluster. " +
            "Under the hood, the regex is compiled to a <code>java.util.regex.Pattern</code>. " +
            "For convenience, comma (',') is interpreted as interpreted as the regex-choice symbol ('|').";
    public static final Object SOURCE_TOPIC_WHITELIST_DEFAULT = ConfigDef.NO_DEFAULT_VALUE;

    public static String SOURCE_TOPIC_MAPPING_CONFIG = "topic.mapping";
    public static final String SOURCE_TOPIC_MAPPING_DOC = "Topic mappings indicating the source topic in source cluster and" +
            "the mapping topic in the target cluster e.g source_topic_test=target_topic_test." +
            "this means the topic source_topic_test will be named as target_topic_test in the target cluster";
    public static final Object SOURCE_TOPIC_MAPPING_DEFAULT = ConfigDef.NO_DEFAULT_VALUE;

    public static String TOPIC_PARTITIONS = "topic.partitions";


    // Internal Connector Timing
    public static final String POLL_LOOP_TIMEOUT_MS_CONFIG = "poll.loop.timeout.ms";
    public static final String POLL_LOOP_TIMEOUT_MS_DOC = "Maximum amount of time to wait in each poll loop without data before cancelling the poll and returning control to the worker task";
    public static final int POLL_LOOP_TIMEOUT_MS_DEFAULT = 1000;
    public static final String MAX_SHUTDOWN_WAIT_MS_CONFIG = "max.shutdown.wait.ms";
    public static final String MAX_SHUTDOWN_WAIT_MS_DOC = "Maximum amount of time to wait before forcing the consumer to close";
    public static final int MAX_SHUTDOWN_WAIT_MS_DEFAULT = 2000;

    // General Source Kafka Config - Applies to Consumer and Admin Client if not overridden by CONSUMER_PREFIX or ADMIN_CLIENT_PREFIX
    public static final String SOURCE_BOOTSTRAP_SERVERS_CONFIG = SOURCE_PREFIX.concat(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
    public static final String SOURCE_BOOTSTRAP_SERVERS_DOC = "list of kafka brokers to use to bootstrap the source cluster";
    public static final Object SOURCE_BOOTSTRAP_SERVERS_DEFAULT = ConfigDef.NO_DEFAULT_VALUE;


    // These are the kafka consumer configs we override defaults for
    // Note that *any* kafka consumer config can be set by adding the
    // CONSUMER_PREFIX in front of the standard consumer config strings
    public static final String CONSUMER_MAX_POLL_RECORDS_CONFIG = CONSUMER_PREFIX.concat(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
    public static final String CONSUMER_MAX_POLL_RECORDS_DOC = "Maximum number of records to return from each poll of the consumer";
    public static final int CONSUMER_MAX_POLL_RECORDS_DEFAULT = 500;
    public static final String CONSUMER_AUTO_OFFSET_RESET_CONFIG = CONSUMER_PREFIX.concat(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
    public static final String CONSUMER_AUTO_OFFSET_RESET_DOC = "If there is no stored offset for a partition, where to reset from [earliest|latest].";
    public static final String CONSUMER_AUTO_OFFSET_RESET_DEFAULT = "latest";
    public static final ConfigDef.ValidString CONSUMER_AUTO_OFFSET_RESET_VALIDATOR = ConfigDef.ValidString.in("earliest", "latest");
    public static final String CONSUMER_KEY_DESERIALIZER_CONFIG = CONSUMER_PREFIX.concat(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
    public static final String CONSUMER_KEY_DESERIALIZER_DOC = "Key deserializer to use for the kafka consumers connecting to the source cluster.";
    public static final String CONSUMER_KEY_DESERIALIZER_DEFAULT = ByteArrayDeserializer.class.getName();
    public static final String CONSUMER_VALUE_DESERIALIZER_CONFIG = CONSUMER_PREFIX.concat(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
    public static final String CONSUMER_VALUE_DESERIALIZER_DOC = "Value deserializer to use for the kafka consumers connecting to the source cluster.";
    public static final String CONSUMER_VALUE_DESERIALIZER_DEFAULT = ByteArrayDeserializer.class.getName();
    public static final String CONSUMER_ENABLE_AUTO_COMMIT_CONFIG = CONSUMER_PREFIX.concat(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
    public static final String CONSUMER_ENABLE_AUTO_COMMIT_DOC = "If true the consumer's offset will be periodically committed to the source cluster in the background. " +
            "Note that these offsets are not used to resume the connector (They are stored in the Kafka Connect offset store), but may be useful in monitoring the current offset lag " +
            "of this connector on the source cluster";
    public static final Boolean CONSUMER_ENABLE_AUTO_COMMIT_DEFAULT = false;

    public static final String CONSUMER_GROUP_ID_CONFIG = CONSUMER_PREFIX.concat(ConsumerConfig.GROUP_ID_CONFIG);
    public static final String CONSUMER_GROUP_ID_DOC = "Source Kafka Consumer group id. This must be set if source.enable.auto.commit is set as a group id is required for offset tracking on the source cluster";
    public static final Object CONSUMER_GROUP_ID_DEFAULT = ConfigDef.NO_DEFAULT_VALUE;

    //Config Definition
    public static final ConfigDef CONFIG = new ConfigDef()
            .define(SOURCE_TOPIC_WHITELIST_CONFIG, ConfigDef.Type.STRING, SOURCE_TOPIC_WHITELIST_DEFAULT, TOPIC_WHITELIST_REGEX_VALIDATOR, ConfigDef.Importance.HIGH, SOURCE_TOPIC_WHITELIST_DOC)
            .define(POLL_LOOP_TIMEOUT_MS_CONFIG, ConfigDef.Type.INT, POLL_LOOP_TIMEOUT_MS_DEFAULT, ConfigDef.Importance.LOW, POLL_LOOP_TIMEOUT_MS_DOC)
            .define(MAX_SHUTDOWN_WAIT_MS_CONFIG, ConfigDef.Type.INT, MAX_SHUTDOWN_WAIT_MS_DEFAULT, ConfigDef.Importance.LOW, MAX_SHUTDOWN_WAIT_MS_DOC)
            .define(SOURCE_BOOTSTRAP_SERVERS_CONFIG, ConfigDef.Type.LIST, SOURCE_BOOTSTRAP_SERVERS_DEFAULT, NON_EMPTY_LIST_VALIDATOR, ConfigDef.Importance.HIGH, SOURCE_BOOTSTRAP_SERVERS_DOC)
            .define(SOURCE_TOPIC_MAPPING_CONFIG, ConfigDef.Type.STRING, SOURCE_TOPIC_MAPPING_DEFAULT, ConfigDef.Importance.HIGH, SOURCE_TOPIC_MAPPING_DOC)
            .define(CONSUMER_MAX_POLL_RECORDS_CONFIG, ConfigDef.Type.INT, CONSUMER_MAX_POLL_RECORDS_DEFAULT, ConfigDef.Importance.LOW, CONSUMER_MAX_POLL_RECORDS_DOC)
            .define(CONSUMER_AUTO_OFFSET_RESET_CONFIG, ConfigDef.Type.STRING, CONSUMER_AUTO_OFFSET_RESET_DEFAULT, CONSUMER_AUTO_OFFSET_RESET_VALIDATOR, ConfigDef.Importance.MEDIUM, CONSUMER_AUTO_OFFSET_RESET_DOC)
            .define(CONSUMER_KEY_DESERIALIZER_CONFIG, ConfigDef.Type.STRING, CONSUMER_KEY_DESERIALIZER_DEFAULT, ConfigDef.Importance.LOW, CONSUMER_KEY_DESERIALIZER_DOC)
            .define(CONSUMER_VALUE_DESERIALIZER_CONFIG, ConfigDef.Type.STRING, CONSUMER_VALUE_DESERIALIZER_DEFAULT, ConfigDef.Importance.LOW, CONSUMER_VALUE_DESERIALIZER_DOC)
            .define(CONSUMER_ENABLE_AUTO_COMMIT_CONFIG, ConfigDef.Type.BOOLEAN, CONSUMER_ENABLE_AUTO_COMMIT_DEFAULT, ConfigDef.Importance.LOW, CONSUMER_ENABLE_AUTO_COMMIT_DOC)
            .define(CONSUMER_GROUP_ID_CONFIG, ConfigDef.Type.STRING, CONSUMER_GROUP_ID_DEFAULT, NON_EMPTY_STRING_VALIDATOR, ConfigDef.Importance.MEDIUM, CONSUMER_GROUP_ID_DOC);


    public KafkaSourceConnectorConfig(Map<String, String> props) {
        super(CONFIG, props);
    }

    // Returns all values with a specified prefix with the prefix stripped from the key
    private Map<String, Object> allWithPrefix(String prefix) {
        return allWithPrefix(prefix, true);
    }

    private Map<String, Object> allWithPrefix(String prefix, boolean stripPrefix) {
        Map<String, Object> result = originalsWithPrefix(prefix);
        for (Map.Entry<String, Object> entry : result.entrySet()) {
            if (entry.getKey().startsWith(prefix) && entry.getKey().length() > prefix.length()) {
                if (stripPrefix) {
                    result.put(entry.getKey().substring(prefix.length()), entry.getValue());
                } else {
                    result.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return result;
    }

    // Returns all values (part of definition or original strings) as strings so they can be used with functions accepting Map<String,String> configs
    public Map<String, String> allAsStrings() {
        Map<String, String> result = new HashMap<>();
        result.put(SOURCE_BOOTSTRAP_SERVERS_CONFIG, String.join(",", getList(SOURCE_BOOTSTRAP_SERVERS_CONFIG)));
        result.put(SOURCE_TOPIC_MAPPING_CONFIG, getString(SOURCE_TOPIC_MAPPING_CONFIG));
        result.put(POLL_LOOP_TIMEOUT_MS_CONFIG, String.valueOf(getInt(POLL_LOOP_TIMEOUT_MS_CONFIG)));
        result.put(MAX_SHUTDOWN_WAIT_MS_CONFIG, String.valueOf(getInt(MAX_SHUTDOWN_WAIT_MS_CONFIG)));
        result.put(CONSUMER_MAX_POLL_RECORDS_CONFIG, String.valueOf(getInt(CONSUMER_MAX_POLL_RECORDS_CONFIG)));
        result.put(CONSUMER_AUTO_OFFSET_RESET_CONFIG, getString(CONSUMER_AUTO_OFFSET_RESET_CONFIG));
        result.put(CONSUMER_KEY_DESERIALIZER_CONFIG, getString(CONSUMER_KEY_DESERIALIZER_CONFIG));
        result.put(CONSUMER_VALUE_DESERIALIZER_CONFIG, getString(CONSUMER_VALUE_DESERIALIZER_CONFIG));
        result.put(CONSUMER_ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(getBoolean(CONSUMER_ENABLE_AUTO_COMMIT_CONFIG)));
        result.putAll(originalsStrings()); // Will set any values without defaults and will capture additional configs like consumer settings if supplied
        return result;
    }

    // Return a Properties Object that can be passed to KafkaConsumer
    public Properties getKafkaConsumerProperties() {
        Properties kafkaConsumerProps = new Properties();

        kafkaConsumerProps.putAll(allWithPrefix(SOURCE_PREFIX));

        kafkaConsumerProps.putAll(allWithPrefix(CONSUMER_PREFIX));
        return kafkaConsumerProps;
    }

    private static Pattern getTopicWhitelistPattern(String rawRegex) {
        String regex = rawRegex
                .trim()
                .replace(',', '|')
                .replace(" ", "")
                .replaceAll("^[\"']+", "")
                .replaceAll("[\"']+$", ""); // property files may bring quotes
        try {
            return Pattern.compile(regex);
        } catch (PatternSyntaxException e) {
            throw new ConfigException(regex + " is an invalid regex for CONFIG " + SOURCE_TOPIC_WHITELIST_CONFIG);
        }
    }

    public Pattern getTopicPattern() {
        return getTopicWhitelistPattern(getString(SOURCE_TOPIC_WHITELIST_CONFIG));
    }
}
