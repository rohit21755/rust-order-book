package com.hft.kafka.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

@Data
@ConfigurationProperties(prefix = "hft.kafka")
public class SharedKafkaProperties {

    /** comma-separated broker list, e.g. localhost:29092 */
    private String bootstrapServers = "localhost:29092";

    /** Service name embedded in headers + consumer group prefix. */
    private String serviceOrigin = "unknown";

    private Producer producer = new Producer();
    private Consumer consumer = new Consumer();
    private Dlq dlq = new Dlq();
    private Admin admin = new Admin();

    @Data
    public static class Producer {
        private String acks = "all";
        private boolean idempotence = true;
        private int retries = Integer.MAX_VALUE;          // client-level retries safe because idempotent
        private int maxInFlightPerConnection = 5;
        private int requestTimeoutMs = 5000;
        private int deliveryTimeoutMs = 30000;
        private int lingerMs = 5;
        private String compressionType = "lz4";
        /** Set non-null to enable transactional producer (exactly-once). */
        private String transactionalId = null;
        private Map<String, String> extraProps = new HashMap<>();
    }

    @Data
    public static class Consumer {
        private boolean readCommitted = false;            // set true for EOS pipelines
        private int maxPollRecords = 500;
        private int maxInFlight = 32;                     // backpressure on processing
        private int maxRetries = 3;
        private long retryBackoffMs = 200L;
        private String autoOffsetReset = "latest";
        private boolean autoCommit = false;               // manual commit after processing
        private int sessionTimeoutMs = 10000;
        private int heartbeatIntervalMs = 3000;
        private Map<String, String> extraProps = new HashMap<>();
    }

    @Data
    public static class Dlq {
        private String topicPrefix = "dlq.";
        private boolean enabled = true;
        /** DLQMonitor will subscribe to this pattern. */
        private String monitorPattern = "dlq\\..*";
        private String monitorGroup = "dlq-monitor-group";
    }

    @Data
    public static class Admin {
        private int defaultPartitions = 3;
        private short defaultReplicationFactor = 1;
        private long topicCreationTimeoutMs = 10000L;
    }
}
