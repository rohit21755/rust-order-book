package com.hft.kafka.dlq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.kafka.config.KafkaReceiverFactory;
import com.hft.kafka.config.SharedKafkaProperties;
import com.hft.kafka.event.DLQMessage;
import com.hft.kafka.headers.HeaderKeys;
import com.hft.kafka.headers.MessageHeaders;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;

/**
 * Reads a DLQ topic and republishes the wrapped originalPayload back to its original topic.
 *
 * <p>Bounded: stops after {@code maxMessages} or {@code timeout}; whichever first. Designed for
 * operator-initiated replay invoked via {@link DLQAdminController}.
 */
@RequiredArgsConstructor
public class DLQReplayService {

    private static final Logger log = LoggerFactory.getLogger(DLQReplayService.class);

    private final KafkaReceiverFactory receiverFactory;
    private final KafkaSender<String, byte[]> sender;
    private final ObjectMapper objectMapper;
    private final SharedKafkaProperties props;

    public Mono<Integer> replay(String dlqTopic, int maxMessages, Duration timeout) {
        if (!dlqTopic.startsWith(props.getDlq().getTopicPrefix())) {
            return Mono.error(new IllegalArgumentException("Not a DLQ topic: " + dlqTopic));
        }
        String replayGroup = "dlq-replay-" + UUID.randomUUID();
        var options = receiverFactory.options(java.util.List.of(dlqTopic), replayGroup);

        return KafkaReceiver.create(options).receive()
                .take(maxMessages)
                .take(timeout)
                .flatMap(rec -> {
                    try {
                        DLQMessage dlq = objectMapper.readValue(rec.value(), DLQMessage.class);
                        byte[] payload = dlq.originalPayload().getBytes(StandardCharsets.UTF_8);
                        Headers h = MessageHeaders.seed(props.getServiceOrigin() + "-replay");
                        MessageHeaders.put(h, HeaderKeys.RETRY_COUNT, "0");
                        MessageHeaders.put(h, "x-replayed-from-dlq", dlqTopic);
                        ProducerRecord<String, byte[]> pr = new ProducerRecord<>(
                                dlq.originalTopic(), null, null, dlq.originalKey(), payload, h);
                        return sender.send(Mono.just(SenderRecord.create(pr, dlq.originalKey())))
                                .next()
                                .doOnSuccess(r -> rec.receiverOffset().acknowledge())
                                .thenReturn(1);
                    } catch (Exception e) {
                        log.warn("Replay parse failure {} : {}", dlqTopic, e.getMessage());
                        rec.receiverOffset().acknowledge();
                        return Flux.<Integer>empty();
                    }
                })
                .reduce(0, Integer::sum);
    }
}
