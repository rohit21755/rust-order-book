package com.hft.kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.kafka.config.KafkaJacksonConfig;
import com.hft.kafka.config.KafkaSenderFactory;
import com.hft.kafka.config.SharedKafkaProperties;
import com.hft.kafka.event.OrderEvent;
import com.hft.kafka.headers.HeaderKeys;
import com.hft.kafka.headers.MessageHeaders;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import reactor.kafka.sender.KafkaSender;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaEventPublisherTest {

    private static final String TOPIC = "test.orders";

    EmbeddedKafkaBroker broker;
    SharedKafkaProperties props;
    KafkaSender<String, byte[]> sender;
    KafkaEventPublisher<OrderEvent> publisher;
    ObjectMapper mapper;

    @BeforeEach
    void setUp() {
        broker = new EmbeddedKafkaBroker(1, true, 1, TOPIC);
        broker.afterPropertiesSet();

        props = new SharedKafkaProperties();
        props.setBootstrapServers(broker.getBrokersAsString());
        props.setServiceOrigin("test-service");

        sender = new KafkaSenderFactory(props).create();
        mapper = new KafkaJacksonConfig().kafkaObjectMapper();
        publisher = new KafkaEventPublisher<>(sender, mapper, props);
    }

    @AfterEach
    void tearDown() {
        if (sender != null) sender.close();
        if (broker != null) broker.destroy();
    }

    @Test
    void publishStampsHeadersAndKey() {
        OrderEvent e = new OrderEvent(OrderEvent.Type.NEW, UUID.randomUUID(), UUID.randomUUID(),
                "BTC-USDT", OrderEvent.Side.BUY, OrderEvent.OrderType.LIMIT,
                new BigDecimal("100"), null, new BigDecimal("1"),
                "idem", Instant.now());

        publisher.publish(TOPIC, e).block(Duration.ofSeconds(10));

        try (KafkaConsumer<String, byte[]> consumer = buildConsumer()) {
            consumer.subscribe(List.of(TOPIC));
            var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10));
            assertThat(records.count()).isEqualTo(1);
            ConsumerRecord<String, byte[]> r = records.iterator().next();
            assertThat(r.key()).isEqualTo(e.orderId().toString());
            assertThat(MessageHeaders.get(r.headers(), HeaderKeys.SERVICE_ORIGIN)).isEqualTo("test-service");
            assertThat(MessageHeaders.get(r.headers(), HeaderKeys.TRACE_ID)).isNotBlank();
            assertThat(MessageHeaders.get(r.headers(), HeaderKeys.SPAN_ID)).isNotBlank();
            assertThat(MessageHeaders.get(r.headers(), HeaderKeys.TIMESTAMP)).isNotBlank();
            assertThat(MessageHeaders.get(r.headers(), HeaderKeys.EVENT_TYPE)).isEqualTo("OrderEvent");
        }
    }

    private KafkaConsumer<String, byte[]> buildConsumer() {
        Properties p = new Properties();
        p.putAll(KafkaTestUtils.consumerProps("test-grp", "true", broker));
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(p);
    }
}
