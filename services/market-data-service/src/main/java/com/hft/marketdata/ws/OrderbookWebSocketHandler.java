package com.hft.marketdata.ws;

import com.hft.marketdata.config.MarketDataProperties;
import com.hft.marketdata.fanout.MarketDataFanout;
import com.hft.marketdata.fanout.SubscriptionRegistry;
import com.hft.marketdata.model.StreamType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;

import java.time.Duration;

/** ws://host/ws/orderbook/{symbol} — top-N orderbook, sampled every 100ms. */
@Component
public class OrderbookWebSocketHandler extends AbstractStreamWebSocketHandler {

    private final MarketDataFanout fanout;

    public OrderbookWebSocketHandler(MarketDataProperties props, SubscriptionRegistry registry,
                                     JsonCodec codec, MarketDataFanout fanout) {
        super(props, registry, codec);
        this.fanout = fanout;
    }

    @Override
    protected String channelKey(WebSocketSession session) {
        return StreamType.ORDERBOOK + ":" + lastPathSegment(session);
    }

    @Override
    protected Flux<Object> payloadStream(WebSocketSession session) {
        String symbol = lastPathSegment(session);
        return fanout.stream(StreamType.ORDERBOOK, symbol)
                .sample(Duration.ofMillis(props.getWs().getOrderbookPushMs()));
    }
}
