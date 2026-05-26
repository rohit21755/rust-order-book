package com.hft.marketdata.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.kafka.config.KafkaReceiverFactory;
import com.hft.kafka.config.SharedKafkaProperties;
import com.hft.kafka.consumer.KafkaEventConsumer;
import com.hft.kafka.dlq.DLQRouter;
import com.hft.kafka.event.PortfolioEvent;
import com.hft.marketdata.candle.CandleAggregator;
import com.hft.marketdata.clickhouse.TradeBatchWriter;
import com.hft.marketdata.config.MarketDataProperties;
import com.hft.marketdata.fanout.MarketDataFanout;
import com.hft.marketdata.model.OrderbookMessage;
import com.hft.marketdata.model.StreamType;
import com.hft.marketdata.model.TradeMessage;
import com.hft.marketdata.redis.RecentTradesCache;
import com.hft.marketdata.ticker.TickerService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

/**
 * One Kafka consumer per topic → fan-out to N WebSocket clients via {@link MarketDataFanout}.
 * Consumer groups auto-named {service-origin}-{topic}-group by the shared receiver factory.
 */
@Slf4j
@Component
public class MarketDataConsumers {

    private final KafkaReceiverFactory receiverFactory;
    private final ObjectMapper mapper;
    private final SharedKafkaProperties kafkaProps;
    private final DLQRouter dlqRouter;
    private final MarketDataProperties props;

    private final MarketDataFanout fanout;
    private final TickerService tickerService;
    private final RecentTradesCache recentTrades;
    private final TradeBatchWriter tradeWriter;
    private final CandleAggregator candleAggregator;

    private final List<Disposable> subscriptions = new ArrayList<>();

    public MarketDataConsumers(KafkaReceiverFactory receiverFactory,
                               @Qualifier("kafkaObjectMapper") ObjectMapper mapper,
                               SharedKafkaProperties kafkaProps,
                               DLQRouter dlqRouter,
                               MarketDataProperties props,
                               MarketDataFanout fanout,
                               TickerService tickerService,
                               RecentTradesCache recentTrades,
                               TradeBatchWriter tradeWriter,
                               CandleAggregator candleAggregator) {
        this.receiverFactory = receiverFactory;
        this.mapper = mapper;
        this.kafkaProps = kafkaProps;
        this.dlqRouter = dlqRouter;
        this.props = props;
        this.fanout = fanout;
        this.tickerService = tickerService;
        this.recentTrades = recentTrades;
        this.tradeWriter = tradeWriter;
        this.candleAggregator = candleAggregator;
    }

    @PostConstruct
    public void start() {
        subscriptions.add(startTrades());
        subscriptions.add(startOrderbook());
        subscriptions.add(startPortfolio());
        log.info("market-data Kafka consumers started");
    }

    @PreDestroy
    public void stop() {
        subscriptions.forEach(Disposable::dispose);
    }

    private Disposable startTrades() {
        var consumer = new KafkaEventConsumer<>(
                receiverFactory.options(props.getKafka().getTradesTopic()),
                mapper, TradeMessage.class, kafkaProps, dlqRouter);
        return consumer.start(msg -> {
            TradeMessage t = msg.payload();
            // 1) fan-out live trade
            fanout.emit(StreamType.TRADES, t.symbol(), t.toView());
            // 2) ticker update → fan-out ticker
            // 3) cache + ClickHouse buffer + candle aggregation
            return tickerService.onTrade(t)
                    .doOnNext(ticker -> fanout.emit(StreamType.TICKER, t.symbol(), ticker))
                    .then(recentTrades.push(t.toView()))
                    .doOnSuccess(v -> {
                        tradeWriter.enqueue(t);
                        candleAggregator.onTrade(t);
                    })
                    .then();
        });
    }

    private Disposable startOrderbook() {
        var consumer = new KafkaEventConsumer<>(
                receiverFactory.options(props.getKafka().getOrderbookTopic()),
                mapper, OrderbookMessage.class, kafkaProps, dlqRouter);
        int depth = props.getWs().getOrderbookDepth();
        return consumer.start(msg -> {
            OrderbookMessage ob = msg.payload();
            fanout.emit(StreamType.ORDERBOOK, ob.symbol(), ob.toView(depth));
            return Mono.empty();
        });
    }

    private Disposable startPortfolio() {
        var consumer = new KafkaEventConsumer<>(
                receiverFactory.options(props.getKafka().getPortfolioTopic()),
                mapper, PortfolioEvent.class, kafkaProps, dlqRouter);
        return consumer.start(msg -> {
            PortfolioEvent pe = msg.payload();
            if (pe.userId() != null) {
                fanout.emit(StreamType.PORTFOLIO, pe.userId().toString(), pe);
            }
            return Mono.empty();
        });
    }
}
