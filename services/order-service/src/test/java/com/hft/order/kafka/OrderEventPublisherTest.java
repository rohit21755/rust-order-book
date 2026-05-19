package com.hft.order.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.order.config.OrderProperties;
import com.hft.shared.error.BusinessException;
import com.hft.shared.error.ErrorCode;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderResult;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OrderEventPublisherTest {

    private KafkaSender<String, String> sender;
    private OrderProperties props;
    private OrderEventPublisher publisher;

    @BeforeEach
    void setUp() {
        sender = mock(KafkaSender.class);
        props = new OrderProperties();
        props.getKafka().setOrdersTopic("orders");
        props.getKafka().setDlqTopic("orders.DLQ");
        props.getKafka().setPublishRetries(2);
        props.getKafka().setPublishBackoffMillis(1);
        publisher = new OrderEventPublisher(sender, new ObjectMapper(), props);
    }

    @SuppressWarnings("unchecked")
    private SenderResult<String> success() {
        SenderResult<String> r = mock(SenderResult.class);
        when(r.exception()).thenReturn(null);
        when(r.recordMetadata()).thenReturn(new RecordMetadata(new TopicPartition("orders", 0), 0, 0, 0, 0, 0));
        return r;
    }

    @SuppressWarnings("unchecked")
    private SenderResult<String> failure(Throwable t) {
        SenderResult<String> r = mock(SenderResult.class);
        when(r.exception()).thenReturn((Exception) t);
        return r;
    }

    private OrderEvent sampleEvent() {
        return new OrderEvent(OrderEvent.Type.NEW, UUID.randomUUID(), UUID.randomUUID(),
                "BTC-USDT", "BUY", "LIMIT", new BigDecimal("10"), null,
                new BigDecimal("1"), Instant.now(), "idem");
    }

    @Test
    void publishesSuccessfully() {
        when(sender.send(any(org.reactivestreams.Publisher.class))).thenReturn(Flux.just(success()));
        StepVerifier.create(publisher.publish(sampleEvent())).verifyComplete();
    }

    @Test
    void retriesThenSucceeds() {
        AtomicInteger calls = new AtomicInteger();
        when(sender.send(any(org.reactivestreams.Publisher.class))).thenAnswer(inv ->
                Flux.defer(() -> calls.incrementAndGet() < 2
                        ? Flux.just(failure(new RuntimeException("transient")))
                        : Flux.just(success())));
        StepVerifier.create(publisher.publish(sampleEvent())).verifyComplete();
    }

    @Test
    void exhaustedRetriesGoToDlqAndError() {
        when(sender.send(any(org.reactivestreams.Publisher.class)))
                .thenReturn(Flux.just(failure(new RuntimeException("boom"))));
        StepVerifier.create(publisher.publish(sampleEvent()))
                .expectErrorMatches(t -> t instanceof BusinessException be
                        && be.getCode() == ErrorCode.KAFKA_PUBLISH_FAILED)
                .verify();
    }
}
