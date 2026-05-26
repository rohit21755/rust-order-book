package com.hft.kafka.dlq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.kafka.config.KafkaReceiverFactory;
import com.hft.kafka.config.SharedKafkaProperties;
import com.hft.kafka.event.DLQMessage;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.kafka.receiver.KafkaReceiver;

import java.util.regex.Pattern;

/** Consumes all {@code dlq.*} topics; logs each dead letter and increments metrics. */
@RequiredArgsConstructor
public class DLQMonitor {

    private static final Logger log = LoggerFactory.getLogger(DLQMonitor.class);

    private final KafkaReceiverFactory receiverFactory;
    private final ObjectMapper objectMapper;
    private final SharedKafkaProperties props;
    private final MeterRegistry meterRegistry;

    private Disposable subscription;

    @PostConstruct
    public void start() {
        if (!props.getDlq().isEnabled()) {
            log.info("DLQMonitor disabled");
            return;
        }
        Pattern p = Pattern.compile(props.getDlq().getMonitorPattern());
        var options = receiverFactory.patternOptions(p, props.getDlq().getMonitorGroup());

        subscription = KafkaReceiver.create(options).receive()
                .doOnNext(rec -> {
                    try {
                        DLQMessage dlq = objectMapper.readValue(rec.value(), DLQMessage.class);
                        Counter.builder("kafka.dlq.received")
                                .tag("topic", rec.topic())
                                .tag("origin", dlq.originalTopic())
                                .tag("errorClass", String.valueOf(dlq.errorClass()))
                                .register(meterRegistry)
                                .increment();
                        log.error("DEAD LETTER topic={} origin={} key={} error={} retries={}",
                                rec.topic(), dlq.originalTopic(), dlq.originalKey(),
                                dlq.errorMessage(), dlq.retryCount());
                    } catch (Exception e) {
                        log.warn("DLQ parse failure topic={} : {}", rec.topic(), e.getMessage());
                    } finally {
                        rec.receiverOffset().acknowledge();
                    }
                })
                .doOnError(err -> log.error("DLQMonitor stream error", err))
                .subscribe();
    }

    @PreDestroy
    public void stop() {
        if (subscription != null) subscription.dispose();
    }
}
