package com.hft.marketdata.ws;

import com.hft.marketdata.config.MarketDataProperties;
import com.hft.marketdata.fanout.MarketDataFanout;
import com.hft.marketdata.fanout.SubscriptionRegistry;
import com.hft.marketdata.model.StreamType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;

/** ws://host/ws/trades/{symbol} — live trade executions as they happen. */
@Component
public class TradesWebSocketHandler extends AbstractStreamWebSocketHandler {

    private final MarketDataFanout fanout;

    public TradesWebSocketHandler(MarketDataProperties props, SubscriptionRegistry registry,
                                  JsonCodec codec, MarketDataFanout fanout) {
        super(props, registry, codec);
        this.fanout = fanout;
    }

    @Override
    protected String channelKey(WebSocketSession session) {
        return StreamType.TRADES + ":" + lastPathSegment(session);
    }

    @Override
    protected Flux<Object> payloadStream(WebSocketSession session) {
        return fanout.stream(StreamType.TRADES, lastPathSegment(session));
    }
}
