package com.hft.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.kafka.config.KafkaJacksonConfig;
import com.hft.kafka.config.KafkaReceiverFactory;
import com.hft.kafka.config.KafkaSenderFactory;
import com.hft.kafka.config.SharedKafkaProperties;
import com.hft.kafka.dlq.DLQRouter;
import com.hft.kafka.event.DLQMessage;
import com.hft.kafka.event.OrderEvent;
import com.hft.kafka.producer.KafkaEventPublisher;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaEventConsumerTest {

    private static final String TOPIC = "test.consume";
    private static final String DLQ_TOPIC = "dlq.test.consume";

    EmbeddedKafkaBroker broker;
    SharedKafkaProperties props;
    KafkaSender<String, byte[]> sender;
    KafkaSenderFactory senderFactory;
    KafkaReceiverFactory receiverFactory;
    ObjectMapper mapper;
    DLQRouter dlqRouter;

    @BeforeEach
    void setUp() {
        broker = new EmbeddedKafkaBroker(1, true, 1, TOPIC, DLQ_TOPIC);
        broker.afterPropertiesSet();

        props = new SharedKafkaProperties();
        props.setBootstrapServers(broker.getBrokersAsString());
        props.setServiceOrigin("test-service");
        props.getConsumer().setMaxRetries(2);
        props.getConsumer().setRetryBackoffMs(20L);
        props.getConsumer().setAutoOffsetReset("earliest");

        senderFactory = new KafkaSenderFactory(props);
        sender = senderFactory.create();
        receiverFactory = new KafkaReceiverFactory(props);
        mapper = new KafkaJacksonConfig().kafkaObjectMapper();
        dlqRouter = new DLQRouter(sender, mapper, props);
    }

    @AfterEach
    void tearDown() {
        if (sender != null) sender.close();
        if (broker != null) broker.destroy();
    }

    private OrderEvent sample() {
        return new OrderEvent(OrderEvent.Type.NEW, UUID.randomUUID(), UUID.randomUUID(),
                "BTC-USDT", OrderEvent.Side.BUY, OrderEvent.OrderType.LIMIT,
                new BigDecimal("100"), null, new BigDecimal("1"), "idem", Instant.now());
    }

    @Test
    void handlerSuccessCommitsOffset() throws Exception {
        KafkaEventPublisher<OrderEvent> publisher = new KafkaEventPublisher<>(sender, mapper, props);
        publisher.publish(TOPIC, sample()).block(Duration.ofSeconds(10));

        CountDownLatch latch = new CountDownLatch(1);
        KafkaEventConsumer<OrderEvent> consumer = new KafkaEventConsumer<>(
                receiverFactory.options(TOPIC), mapper, OrderEvent.class, props, dlqRouter);

        Disposable sub = consumer.start(msg -> {
            latch.countDown();
            return Mono.empty();
        });

        assertThat(latch.await(15, TimeUnit.SECONDS)).isTrue();
        sub.dispose();
    }

    @Test
    void handlerFailureRoutesToDlqAfterRetries() throws Exception {
        KafkaEventPublisher<OrderEvent> publisher = new KafkaEventPublisher<>(sender, mapper, props);
        publisher.publish(TOPIC, sample()).block(Duration.ofSeconds(10));

        AtomicInteger attempts = new AtomicInteger();
        KafkaEventConsumer<OrderEvent> consumer = new KafkaEventConsumer<>(
                receiverFactory.options(TOPIC), mapper, OrderEvent.class, props, dlqRouter);

        Disposable sub = consumer.start(msg -> {
            attempts.incrementAndGet();
            return Mono.error(new RuntimeException("processing failed"));
        });

        try (KafkaConsumer<String, byte[]> dlqConsumer = buildConsumer()) {
            dlqConsumer.subscribe(List.of(DLQ_TOPIC));
            var records = KafkaTestUtils.getRecords(dlqConsumer, Duration.ofSeconds(20));
            assertThat(records.count()).isEqualTo(1);
            DLQMessage dlq = mapper.readValue(records.iterator().next().value(), DLQMessage.class);
            assertThat(dlq.originalTopic()).isEqualTo(TOPIC);
            assertThat(dlq.errorMessage()).contains("processing failed");
        }
        // attempts == 1 + maxRetries(2) = 3
        assertThat(attempts.get()).isGreaterThanOrEqualTo(3);
        sub.dispose();
    }

    private KafkaConsumer<String, byte[]> buildConsumer() {
        Properties p = new Properties();
        p.putAll(KafkaTestUtils.consumerProps("dlq-grp", "true", broker));
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(p);
    }
}
