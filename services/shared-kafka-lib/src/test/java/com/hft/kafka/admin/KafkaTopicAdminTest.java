package com.hft.kafka.admin;

import com.hft.kafka.config.SharedKafkaProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Map;

class KafkaTopicAdminTest {

    EmbeddedKafkaBroker broker;
    SharedKafkaProperties props;
    KafkaTopicAdmin admin;

    @BeforeEach
    void setUp() {
        broker = new EmbeddedKafkaBroker(1, true, 1);
        broker.afterPropertiesSet();
        props = new SharedKafkaProperties();
        props.setBootstrapServers(broker.getBrokersAsString());
        props.setServiceOrigin("test-service");
        admin = new KafkaTopicAdmin(props);
    }

    @AfterEach
    void tearDown() {
        if (admin != null) admin.close();
        if (broker != null) broker.destroy();
    }

    @Test
    void createTopicIfAbsentReturnsTrueFirstTimeFalseSecondTime() {
        String topic = "test.admin.topic";
        StepVerifier.create(admin.createTopicIfAbsent(topic, 1, (short) 1, Map.of()))
                .expectNext(true).verifyComplete();
        StepVerifier.create(admin.createTopicIfAbsent(topic, 1, (short) 1, Map.of()))
                .expectNext(false).verifyComplete();
    }

    @Test
    void existsReportsAccurately() {
        String topic = "test.admin.exists";
        admin.createTopicIfAbsent(topic).block(Duration.ofSeconds(5));
        StepVerifier.create(admin.exists(topic)).expectNext(true).verifyComplete();
        StepVerifier.create(admin.exists("nope.never.created")).expectNext(false).verifyComplete();
    }

    @Test
    void healthCheckSucceedsAgainstLiveBroker() {
        StepVerifier.create(admin.healthCheck())
                .assertNext(r -> {
                    assert r.healthy();
                    assert r.latencyMs() >= 0;
                })
                .verifyComplete();
    }
}
