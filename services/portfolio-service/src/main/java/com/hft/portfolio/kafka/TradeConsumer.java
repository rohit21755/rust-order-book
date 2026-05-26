package com.hft.portfolio.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.kafka.config.KafkaReceiverFactory;
import com.hft.kafka.config.SharedKafkaProperties;
import com.hft.kafka.consumer.KafkaEventConsumer;
import com.hft.kafka.dlq.DLQRouter;
import com.hft.portfolio.config.PortfolioProperties;
import com.hft.portfolio.service.SettlementService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;

/**
 * Consumes the `trades` topic and settles each trade. At-least-once delivery; settlement is
 * idempotent (processed_trades), so re-delivery is safe. Consumer group:
 * portfolio-service-trades-group (auto-named).
 */
@Slf4j
@Component
public class TradeConsumer {

    private final KafkaReceiverFactory receiverFactory;
    private final ObjectMapper mapper;
    private final SharedKafkaProperties kafkaProps;
    private final DLQRouter dlqRouter;
    private final PortfolioProperties props;
    private final SettlementService settlementService;

    private Disposable subscription;

    public TradeConsumer(KafkaReceiverFactory receiverFactory,
                         @Qualifier("kafkaObjectMapper") ObjectMapper mapper,
                         SharedKafkaProperties kafkaProps,
                         DLQRouter dlqRouter,
                         PortfolioProperties props,
                         SettlementService settlementService) {
        this.receiverFactory = receiverFactory;
        this.mapper = mapper;
        this.kafkaProps = kafkaProps;
        this.dlqRouter = dlqRouter;
        this.props = props;
        this.settlementService = settlementService;
    }

    @PostConstruct
    public void start() {
        var consumer = new KafkaEventConsumer<>(
                receiverFactory.options(props.getKafka().getTradesTopic()),
                mapper, TradeMessage.class, kafkaProps, dlqRouter);
        subscription = consumer.start(msg -> settlementService.settle(msg.payload()));
        log.info("portfolio trade consumer started topic={}", props.getKafka().getTradesTopic());
    }

    @PreDestroy
    public void stop() {
        if (subscription != null) subscription.dispose();
    }
}
