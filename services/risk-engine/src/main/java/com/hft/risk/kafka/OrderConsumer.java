package com.hft.risk.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.kafka.config.KafkaReceiverFactory;
import com.hft.kafka.config.SharedKafkaProperties;
import com.hft.kafka.consumer.KafkaEventConsumer;
import com.hft.kafka.dlq.DLQRouter;
import com.hft.risk.config.RiskProperties;
import com.hft.risk.service.RiskEvaluator;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;

/** Consumes the orders topic; each NEW order runs through RiskEvaluator. */
@Slf4j
@Component
public class OrderConsumer {

    private final KafkaReceiverFactory receiverFactory;
    private final ObjectMapper mapper;
    private final SharedKafkaProperties kafkaProps;
    private final DLQRouter dlqRouter;
    private final RiskProperties props;
    private final RiskEvaluator evaluator;

    private Disposable subscription;

    public OrderConsumer(KafkaReceiverFactory receiverFactory,
                         @Qualifier("kafkaObjectMapper") ObjectMapper mapper,
                         SharedKafkaProperties kafkaProps,
                         DLQRouter dlqRouter,
                         RiskProperties props,
                         RiskEvaluator evaluator) {
        this.receiverFactory = receiverFactory;
        this.mapper = mapper;
        this.kafkaProps = kafkaProps;
        this.dlqRouter = dlqRouter;
        this.props = props;
        this.evaluator = evaluator;
    }

    @PostConstruct
    public void start() {
        var consumer = new KafkaEventConsumer<>(
                receiverFactory.options(props.getKafka().getOrdersTopic()),
                mapper, OrderMessage.class, kafkaProps, dlqRouter);
        subscription = consumer.start(msg -> evaluator.evaluate(msg.payload()).then());
        log.info("risk order consumer started topic={}", props.getKafka().getOrdersTopic());
    }

    @PreDestroy
    public void stop() {
        if (subscription != null) subscription.dispose();
    }
}
