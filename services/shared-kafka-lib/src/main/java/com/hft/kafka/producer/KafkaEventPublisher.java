package com.hft.kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.kafka.config.SharedKafkaProperties;
import com.hft.kafka.event.EventEnvelope;
import com.hft.kafka.headers.MessageHeaders;
import com.hft.kafka.headers.TracePropagation;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Map;

/**
 * Generic reactive event publisher.
 *
 * <p>Per call: serializes payload to JSON, stamps standard headers (traceId / spanId / timestamp
 * / service-origin / event-type), and dispatches to the supplied topic.
 *
 * <p>Underlying Kafka producer already retries (idempotent + retries=MAX_VALUE). This wrapper
 * adds one additional bounded {@link Retry#backoff} envelope so caller code can react to
 * non-recoverable failures.
 *
 * @param <T> event payload type implementing {@link EventEnvelope}
 */
@RequiredArgsConstructor
public class KafkaEventPublisher<T extends EventEnvelope> {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventPublisher.class);

    private final KafkaSender<String, byte[]> sender;
    private final ObjectMapper objectMapper;
    private final SharedKafkaProperties props;

    /** Publish event to topic; returns RecordMetadata on success. */
    public Mono<RecordMetadata> publish(String topic, T event) {
        return publish(topic, event, null, null);
    }

    public Mono<RecordMetadata> publish(String topic, T event,
                                        Map<String, String> extraHeaders) {
        return publish(topic, event, extraHeaders, null);
    }

    /**
     * @param traceId  optional caller-provided trace id; generated if null.
     */
    public Mono<RecordMetadata> publish(String topic, T event,
                                        Map<String, String> extraHeaders,
                                        String traceId) {
        return Mono.fromCallable(() -> objectMapper.writeValueAsBytes(event))
                .flatMap(payload -> doSend(topic, event, payload, extraHeaders, traceId))
                .retryWhen(Retry.backoff(2, Duration.ofMillis(200))
                        .filter(this::isRetryable)
                        .doBeforeRetry(s -> log.warn("Kafka publish retry #{} topic={} key={}: {}",
                                s.totalRetries() + 1, topic, event.partitionKey(), s.failure().getMessage())));
    }

    private Mono<RecordMetadata> doSend(String topic, T event, byte[] payload,
                                        Map<String, String> extraHeaders, String traceId) {
        String key = event.partitionKey();
        Headers headers = (traceId == null)
                ? MessageHeaders.seed(props.getServiceOrigin())
                : MessageHeaders.seed(props.getServiceOrigin(), traceId,
                        Long.toHexString(System.nanoTime()));
        MessageHeaders.withType(headers, event.getClass().getSimpleName());
        MessageHeaders.putAll(headers, extraHeaders);
        // W3C Trace Context: inject traceparent/tracestate into outgoing record.
        TracePropagation.inject(headers);

        ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                topic, null, null, key, payload, headers);

        return sender.send(Mono.just(SenderRecord.create(record, key)))
                .next()
                .flatMap(result -> {
                    if (result.exception() != null) {
                        return Mono.error(result.exception());
                    }
                    return Mono.just(result.recordMetadata());
                });
    }

    private boolean isRetryable(Throwable t) {
        // org.apache.kafka.common.errors.RetriableException is already handled by the producer;
        // here we only filter out hard failures.
        return !(t instanceof org.apache.kafka.common.errors.AuthenticationException)
                && !(t instanceof org.apache.kafka.common.errors.AuthorizationException)
                && !(t instanceof org.apache.kafka.common.errors.SerializationException);
    }
}
