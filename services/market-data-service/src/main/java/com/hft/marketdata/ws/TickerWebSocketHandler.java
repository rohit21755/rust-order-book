package com.hft.marketdata.ws;

import com.hft.marketdata.config.MarketDataProperties;
import com.hft.marketdata.fanout.MarketDataFanout;
import com.hft.marketdata.fanout.SubscriptionRegistry;
import com.hft.marketdata.model.StreamType;
import com.hft.marketdata.ticker.TickerService;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;

import java.time.Duration;

/** ws://host/ws/ticker/{symbol} — ticker snapshot, sampled every second. */
@Component
public class TickerWebSocketHandler extends AbstractStreamWebSocketHandler {

    private final MarketDataFanout fanout;
    private final TickerService tickerService;

    public TickerWebSocketHandler(MarketDataProperties props, SubscriptionRegistry registry,
                                  JsonCodec codec, MarketDataFanout fanout, TickerService tickerService) {
        super(props, registry, codec);
        this.fanout = fanout;
        this.tickerService = tickerService;
    }

    @Override
    protected String channelKey(WebSocketSession session) {
        return StreamType.TICKER + ":" + lastPathSegment(session);
    }

    @Override
    protected Flux<Object> payloadStream(WebSocketSession session) {
        String symbol = lastPathSegment(session);
        // Push a periodic snapshot every second (decoupled from trade rate).
        return Flux.interval(Duration.ofMillis(props.getWs().getTickerPushMs()))
                .flatMap(i -> tickerService.current(symbol))
                .cast(Object.class)
                .mergeWith(fanout.stream(StreamType.TICKER, symbol)
                        .sample(Duration.ofMillis(props.getWs().getTickerPushMs())));
    }
}
