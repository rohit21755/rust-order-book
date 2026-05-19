package com.hft.order.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.order.config.OrderProperties;
import com.hft.shared.error.BusinessException;
import com.hft.shared.error.ErrorCode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.util.retry.Retry;

import java.time.Duration;

/**
 * Reactive Kafka publisher.
 * - Key = orderId (partition ordering per order).
 * - Retries 3x with exponential backoff. After exhaustion → DLQ topic, then signal failure.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventPublisher {

    private final KafkaSender<String, String> sender;
    private final ObjectMapper objectMapper;
    private final OrderProperties props;

    public Mono<Void> publish(OrderEvent event) {
        return Mono.fromCallable(() -> objectMapper.writeValueAsString(event))
                .flatMap(payload -> doSend(props.getKafka().getOrdersTopic(), event.orderId().toString(), payload))
                .retryWhen(Retry.backoff(props.getKafka().getPublishRetries(),
                                Duration.ofMillis(props.getKafka().getPublishBackoffMillis()))
                        .filter(t -> !(t instanceof BusinessException))
                        .doBeforeRetry(sig -> log.warn("Kafka publish retry {} for order={}: {}",
                                sig.totalRetries() + 1, event.orderId(), sig.failure().getMessage())))
                .onErrorResume(err -> {
                    log.error("Kafka publish exhausted for order={}, routing to DLQ", event.orderId(), err);
                    return routeToDlq(event, err)
                            .then(Mono.error(BusinessException.internal(
                                    ErrorCode.KAFKA_PUBLISH_FAILED, "Failed to publish order event")));
                })
                .then();
    }

    private Mono<Void> doSend(String topic, String key, String payload) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, payload);
        return sender.send(Mono.just(SenderRecord.create(record, key)))
                .next()
                .flatMap(result -> {
                    if (result.exception() != null) return Mono.error(result.exception());
                    return Mono.empty();
                });
    }

    private Mono<Void> routeToDlq(OrderEvent event, Throwable cause) {
        return Mono.fromCallable(() -> objectMapper.writeValueAsString(event))
                .flatMap(payload -> doSend(props.getKafka().getDlqTopic(), event.orderId().toString(), payload))
                .onErrorResume(dlqErr -> {
                    log.error("DLQ publish also failed for order={}", event.orderId(), dlqErr);
                    return Mono.empty();
                });
    }
}
