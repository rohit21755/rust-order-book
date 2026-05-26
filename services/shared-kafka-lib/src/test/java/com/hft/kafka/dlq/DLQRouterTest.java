package com.hft.kafka.dlq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.kafka.config.KafkaJacksonConfig;
import com.hft.kafka.config.KafkaSenderFactory;
import com.hft.kafka.config.SharedKafkaProperties;
import com.hft.kafka.event.DLQMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DLQRouterTest {

    private static final String SOURCE = "test.source";
    private static final String DLQ = "dlq.test.source";

    EmbeddedKafkaBroker broker;
    SharedKafkaProperties props;
    KafkaSender<String, byte[]> sender;
    ObjectMapper mapper;
    DLQRouter router;

    @BeforeEach
    void setUp() {
        broker = new EmbeddedKafkaBroker(1, true, 1, SOURCE, DLQ);
        broker.afterPropertiesSet();
        props = new SharedKafkaProperties();
        props.setBootstrapServers(broker.getBrokersAsString());
        props.setServiceOrigin("test-service");
        sender = new KafkaSenderFactory(props).create();
        mapper = new KafkaJacksonConfig().kafkaObjectMapper();
        router = new DLQRouter(sender, mapper, props);
    }

    @AfterEach
    void tearDown() {
        if (sender != null) sender.close();
        if (broker != null) broker.destroy();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    void routesFailedRecordToDlq() throws Exception {
        ReceiverRecord<String, byte[]> rec = mock(ReceiverRecord.class);
        when(rec.topic()).thenReturn(SOURCE);
        when(rec.key()).thenReturn("k-1");
        when(rec.value()).thenReturn("{\"x\":1}".getBytes());
        when(rec.headers()).thenReturn(new RecordHeaders());
        when(rec.receiverOffset()).thenReturn(mock(ReceiverOffset.class));

        Mono<?> m = router.route(rec, new RuntimeException("explode"), 3);
        m.block(Duration.ofSeconds(10));

        try (KafkaConsumer<String, byte[]> c = build()) {
            c.subscribe(List.of(DLQ));
            var records = KafkaTestUtils.getRecords(c, Duration.ofSeconds(10));
            assertThat(records.count()).isEqualTo(1);
            DLQMessage dlq = mapper.readValue(records.iterator().next().value(), DLQMessage.class);
            assertThat(dlq.originalTopic()).isEqualTo(SOURCE);
            assertThat(dlq.originalKey()).isEqualTo("k-1");
            assertThat(dlq.errorMessage()).isEqualTo("explode");
            assertThat(dlq.errorClass()).isEqualTo("java.lang.RuntimeException");
            assertThat(dlq.retryCount()).isEqualTo(3);
        }
    }

    private KafkaConsumer<String, byte[]> build() {
        Properties p = new Properties();
        p.putAll(KafkaTestUtils.consumerProps("dlq-router-grp", "true", broker));
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(p);
    }
}
