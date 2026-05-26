package com.hft.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/** Builds reactor-kafka {@link reactor.kafka.receiver.KafkaReceiver} options from {@link SharedKafkaProperties}. */
public class KafkaReceiverFactory {

    private final SharedKafkaProperties props;

    public KafkaReceiverFactory(SharedKafkaProperties props) {
        this.props = props;
    }

    /** Build options subscribed to specific topics. Consumer group naming: {service}-{topic}-group. */
    public ReceiverOptions<String, byte[]> options(String topic) {
        return options(List.of(topic), defaultGroup(topic));
    }

    public ReceiverOptions<String, byte[]> options(Collection<String> topics, String groupId) {
        return baseOptions(groupId).subscription(topics);
    }

    public ReceiverOptions<String, byte[]> patternOptions(Pattern pattern, String groupId) {
        return baseOptions(groupId).subscription(pattern);
    }

    public String defaultGroup(String topic) {
        return props.getServiceOrigin() + "-" + topic + "-group";
    }

    private ReceiverOptions<String, byte[]> baseOptions(String groupId) {
        Map<String, Object> cfg = new HashMap<>();
        SharedKafkaProperties.Consumer c = props.getConsumer();

        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getBootstrapServers());
        cfg.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, c.getAutoOffsetReset());
        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, c.isAutoCommit());
        cfg.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, c.getMaxPollRecords());
        cfg.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, c.getSessionTimeoutMs());
        cfg.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, c.getHeartbeatIntervalMs());

        if (c.isReadCommitted()) {
            cfg.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        }

        cfg.putAll(c.getExtraProps());

        return ReceiverOptions.<String, byte[]>create(cfg);
    }
}
