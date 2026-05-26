package com.hft.marketdata.ws;

import com.hft.marketdata.config.MarketDataProperties;
import com.hft.marketdata.fanout.MarketDataFanout;
import com.hft.marketdata.fanout.SubscriptionRegistry;
import com.hft.marketdata.model.StreamType;
import com.hft.shared.security.JwtTokenValidator;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.UUID;

/**
 * ws://host/ws/portfolio?token=JWT — authenticated portfolio stream.
 * Token passed as query param (browsers cannot set custom WS headers). Validated via shared JWT.
 */
@Component
public class PortfolioWebSocketHandler extends AbstractStreamWebSocketHandler {

    private final MarketDataFanout fanout;
    private final JwtTokenValidator jwtValidator;

    public PortfolioWebSocketHandler(MarketDataProperties props, SubscriptionRegistry registry,
                                     JsonCodec codec, MarketDataFanout fanout, JwtTokenValidator jwtValidator) {
        super(props, registry, codec);
        this.fanout = fanout;
        this.jwtValidator = jwtValidator;
    }

    private UUID userId(WebSocketSession session) {
        String token = tokenFrom(session.getHandshakeInfo().getUri());
        if (token == null) throw new IllegalArgumentException("missing token");
        return jwtValidator.extractUserId(token);
    }

    @Override
    protected Mono<Void> authorize(WebSocketSession session) {
        return Mono.fromCallable(() -> userId(session)).then();
    }

    @Override
    protected String channelKey(WebSocketSession session) {
        return StreamType.PORTFOLIO + ":" + userId(session);
    }

    @Override
    protected Flux<Object> payloadStream(WebSocketSession session) {
        UUID uid = userId(session);
        return fanout.stream(StreamType.PORTFOLIO, uid.toString());
    }

    private static String tokenFrom(URI uri) {
        String query = uri.getQuery();
        if (query == null) return null;
        for (String part : query.split("&")) {
            int eq = part.indexOf('=');
            if (eq > 0 && part.substring(0, eq).equals("token")) {
                return part.substring(eq + 1);
            }
        }
        return null;
    }
}
