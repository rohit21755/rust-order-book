package com.hft.kafka.admin;

import com.hft.kafka.config.SharedKafkaProperties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** Reactive wrapper around AdminClient. Use for testing + bootstrap topic creation. */
public class KafkaTopicAdmin implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaTopicAdmin.class);

    private final AdminClient client;
    private final SharedKafkaProperties props;

    public KafkaTopicAdmin(SharedKafkaProperties props) {
        this.props = props;
        Properties cfg = new Properties();
        cfg.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, props.getBootstrapServers());
        cfg.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) props.getAdmin().getTopicCreationTimeoutMs());
        this.client = AdminClient.create(cfg);
    }

    /** Create topic if absent; resolves with {@code true} if new, {@code false} if pre-existing. */
    public Mono<Boolean> createTopicIfAbsent(String name) {
        return createTopicIfAbsent(name, props.getAdmin().getDefaultPartitions(),
                props.getAdmin().getDefaultReplicationFactor(), Map.of());
    }

    public Mono<Boolean> createTopicIfAbsent(String name, int partitions, short replication,
                                             Map<String, String> configs) {
        NewTopic topic = new NewTopic(name, partitions, replication);
        Map<String, String> cfg = new HashMap<>(configs);
        topic.configs(cfg);
        return Mono.fromCallable(() -> {
            try {
                client.createTopics(List.of(topic)).all()
                        .get(props.getAdmin().getTopicCreationTimeoutMs(),
                                java.util.concurrent.TimeUnit.MILLISECONDS);
                return true;
            } catch (java.util.concurrent.ExecutionException ee) {
                if (ee.getCause() instanceof TopicExistsException) {
                    return false;
                }
                throw ee;
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    public Mono<Boolean> exists(String name) {
        return Mono.fromCallable(() -> {
            ListTopicsResult r = client.listTopics();
            return r.names().get(5, java.util.concurrent.TimeUnit.SECONDS).contains(name);
        }).subscribeOn(Schedulers.boundedElastic());
    }

    /** Cluster reachability + topic-listing probe. Timeout-bounded. */
    public Mono<HealthReport> healthCheck() {
        return healthCheck(Duration.ofSeconds(5));
    }

    public Mono<HealthReport> healthCheck(Duration timeout) {
        return Mono.fromCallable(() -> {
            long t0 = System.nanoTime();
            int topicCount = client.listTopics().names()
                    .get(timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS)
                    .size();
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
            return new HealthReport(true, topicCount, elapsedMs, null);
        })
        .subscribeOn(Schedulers.boundedElastic())
        .onErrorResume(err -> {
            log.warn("Kafka health check failed: {}", err.getMessage());
            return Mono.just(new HealthReport(false, -1, -1, err.getMessage()));
        });
    }

    @Override
    public void close() {
        client.close(Duration.ofSeconds(5));
    }

    public record HealthReport(boolean healthy, int topicCount, long latencyMs, String error) {}
}
