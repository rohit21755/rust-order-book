package com.hft.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.kafka.config.SharedKafkaProperties;
import com.hft.kafka.dlq.DLQRouter;
import com.hft.kafka.headers.HeaderKeys;
import com.hft.kafka.headers.MessageHeaders;
import com.hft.kafka.headers.TracePropagation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.function.Function;

/**
 * Generic reactive consumer with at-least-once semantics.
 *
 * <p>Per record: deserialize payload → invoke handler. On error → retry up to N times with
 * exponential backoff. On final failure → route to {@code dlq.{originalTopic}} via {@link DLQRouter}
 * and commit offset (poison message moves on). On success → commit offset.
 *
 * <p>Backpressure: {@link reactor.kafka.receiver.ReceiverOptions} maxPollRecords controls fetch
 * batch; processing parallelism capped by {@code maxInFlight} from {@link SharedKafkaProperties}.
 *
 * @param <T> deserialized payload type
 */
public class KafkaEventConsumer<T> {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventConsumer.class);

    private final ReceiverOptions<String, byte[]> options;
    private final ObjectMapper objectMapper;
    private final Class<T> payloadType;
    private final SharedKafkaProperties props;
    private final DLQRouter dlqRouter;

    public KafkaEventConsumer(ReceiverOptions<String, byte[]> options,
                              ObjectMapper objectMapper,
                              Class<T> payloadType,
                              SharedKafkaProperties props,
                              DLQRouter dlqRouter) {
        this.options = options;
        this.objectMapper = objectMapper;
        this.payloadType = payloadType;
        this.props = props;
        this.dlqRouter = dlqRouter;
    }

    /**
     * Start the consumer; returns a {@link Disposable} the caller stops on shutdown.
     *
     * @param handler async per-record processor; success completes the Mono, failure triggers retry/DLQ
     */
    public Disposable start(Function<Message<T>, Mono<Void>> handler) {
        SharedKafkaProperties.Consumer c = props.getConsumer();
        int maxInFlight = Math.max(1, c.getMaxInFlight());

        return KafkaReceiver.create(options)
                .receive()
                .flatMap(record -> processWithRetry(record, handler), maxInFlight)
                .subscribe(
                        v -> {},
                        err -> log.error("Consumer pipeline error (re-subscription may be needed)", err)
                );
    }

    private Mono<Void> processWithRetry(ReceiverRecord<String, byte[]> record,
                                        Function<Message<T>, Mono<Void>> handler) {
        SharedKafkaProperties.Consumer c = props.getConsumer();

        Mono<Void> processOnce = Mono.defer(() -> {
            // Extract upstream W3C context + open a CONSUMER span around the handler.
            try (AutoCloseable spanScope = TracePropagation.consumerSpan(record.headers(),
                    "kafka.consume " + record.topic())) {
                T payload = objectMapper.readValue(record.value(), payloadType);
                Message<T> msg = new Message<>(record, payload);
                return handler.apply(msg);
            } catch (Exception ex) {
                return Mono.error(ex);
            }
        });

        return processOnce
                .retryWhen(Retry.backoff(c.getMaxRetries(), Duration.ofMillis(c.getRetryBackoffMs()))
                        .doBeforeRetry(s -> log.warn("Retry #{} topic={} key={} : {}",
                                s.totalRetries() + 1, record.topic(), record.key(), s.failure().getMessage())))
                .onErrorResume(err -> routeToDlq(record, err))
                .doOnSuccess(v -> record.receiverOffset().acknowledge())
                .doOnError(err -> log.error("Final processing failure topic={} key={}", record.topic(), record.key(), err));
    }

    private Mono<Void> routeToDlq(ReceiverRecord<String, byte[]> record, Throwable err) {
        int retryCount = MessageHeaders.getInt(record.headers(), HeaderKeys.RETRY_COUNT, 0)
                + props.getConsumer().getMaxRetries();
        return dlqRouter.route(record, err, retryCount)
                .doOnSuccess(meta -> log.warn("Routed to DLQ topic={} key={} reason={}",
                        record.topic(), record.key(), err.getMessage()))
                .then();
    }

    /** Container for record + deserialized payload exposed to handler functions. */
    public record Message<T>(ConsumerRecord<String, byte[]> record, T payload) {
        public String headerString(String key) { return MessageHeaders.get(record.headers(), key); }
    }
}
