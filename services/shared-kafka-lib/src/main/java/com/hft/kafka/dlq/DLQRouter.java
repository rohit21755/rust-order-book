package com.hft.kafka.dlq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.kafka.config.SharedKafkaProperties;
import com.hft.kafka.event.DLQMessage;
import com.hft.kafka.headers.HeaderKeys;
import com.hft.kafka.headers.MessageHeaders;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

/** Wraps a failed record into a {@link DLQMessage} and publishes to {@code dlq.{originalTopic}}. */
@RequiredArgsConstructor
public class DLQRouter {

    private static final Logger log = LoggerFactory.getLogger(DLQRouter.class);

    private final KafkaSender<String, byte[]> sender;
    private final ObjectMapper objectMapper;
    private final SharedKafkaProperties props;

    public Mono<RecordMetadata> route(ReceiverRecord<String, byte[]> record, Throwable error, int retryCount) {
        String dlqTopic = props.getDlq().getTopicPrefix() + record.topic();
        String original = record.value() == null ? "" : new String(record.value(), StandardCharsets.UTF_8);

        DLQMessage dlq = new DLQMessage(
                record.topic(),
                record.key(),
                original,
                error.getMessage(),
                error.getClass().getName(),
                retryCount,
                Instant.now(),
                MessageHeaders.toMap(record.headers())
        );

        return Mono.fromCallable(() -> objectMapper.writeValueAsBytes(dlq))
                .flatMap(payload -> {
                    Headers h = MessageHeaders.seed(props.getServiceOrigin());
                    MessageHeaders.put(h, HeaderKeys.ORIGINAL_TOPIC, record.topic());
                    MessageHeaders.put(h, HeaderKeys.RETRY_COUNT, String.valueOf(retryCount));
                    MessageHeaders.put(h, HeaderKeys.EVENT_TYPE, "DLQMessage");
                    ProducerRecord<String, byte[]> rec = new ProducerRecord<>(
                            dlqTopic, null, null, dlq.partitionKey(), payload, h);
                    return sender.send(Mono.just(SenderRecord.create(rec, dlq.partitionKey())))
                            .next()
                            .flatMap(result -> result.exception() != null
                                    ? Mono.error(result.exception())
                                    : Mono.just(result.recordMetadata()));
                })
                .onErrorResume(err -> {
                    log.error("DLQ publish failed for topic={} key={}", record.topic(), record.key(), err);
                    return Mono.empty();
                });
    }
}
