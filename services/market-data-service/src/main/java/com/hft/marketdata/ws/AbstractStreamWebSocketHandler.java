package com.hft.marketdata.ws;

import com.hft.marketdata.config.MarketDataProperties;
import com.hft.marketdata.fanout.SubscriptionRegistry;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

/**
 * Base WebSocket handler:
 * - Outbound data Flux gets {@code onBackpressureBuffer(N, DROP_OLDEST)} so a slow client never
 *   blocks the shared publisher.
 * - Server pings every {@code heartbeatIntervalMs}; inbound frames (incl. PONG) reset a staleness
 *   timeout of {@code maxMissedPongs * heartbeatIntervalMs}. No frames in that window → close.
 * - Registers/clears the session in the {@link SubscriptionRegistry}.
 */
@Slf4j
public abstract class AbstractStreamWebSocketHandler implements WebSocketHandler {

    protected final MarketDataProperties props;
    protected final SubscriptionRegistry registry;
    protected final JsonCodec codec;

    protected AbstractStreamWebSocketHandler(MarketDataProperties props,
                                             SubscriptionRegistry registry,
                                             JsonCodec codec) {
        this.props = props;
        this.registry = registry;
        this.codec = codec;
    }

    /** Subclass: the channel key (e.g. "ORDERBOOK:BTC-USDT") for registry + payload source. */
    protected abstract String channelKey(WebSocketSession session);

    /** Subclass: stream of payload objects to serialize and send. */
    protected abstract Flux<Object> payloadStream(WebSocketSession session);

    /** Subclass: validate the handshake (auth). Return Mono.error to reject. Default = allow. */
    protected Mono<Void> authorize(WebSocketSession session) {
        return Mono.empty();
    }

    protected static String lastPathSegment(WebSocketSession session) {
        List<String> segments = List.of(session.getHandshakeInfo().getUri().getPath().split("/"));
        return segments.isEmpty() ? "" : segments.get(segments.size() - 1);
    }

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        return authorize(session).then(Mono.defer(() -> run(session)));
    }

    private Mono<Void> run(WebSocketSession session) {
        String sessionId = session.getId();
        String channel = channelKey(session);
        registry.register(sessionId, channel);

        var ws = props.getWs();

        // Outbound data with per-client backpressure: buffer then drop oldest.
        Flux<WebSocketMessage> data = payloadStream(session)
                .onBackpressureBuffer(
                        ws.getClientBufferSize(),
                        dropped -> log.debug("dropped oldest msg for session={} channel={}", sessionId, channel),
                        BufferOverflowStrategy.DROP_OLDEST)
                .map(codec::toJson)
                .map(session::textMessage);

        // Heartbeat pings.
        Flux<WebSocketMessage> pings = Flux.interval(Duration.ofMillis(ws.getHeartbeatIntervalMs()))
                .map(i -> session.pingMessage(f -> f.wrap("ping".getBytes())));

        Publisher<WebSocketMessage> outbound = Flux.merge(data, pings);

        // Inbound: observe frames (PONG etc.) for liveness; staleness window closes the socket.
        Duration staleWindow = Duration.ofMillis(ws.getHeartbeatIntervalMs() * Math.max(1, ws.getMaxMissedPongs()));
        Mono<Void> inbound = session.receive()
                .timeout(staleWindow)
                .doOnNext(WebSocketMessage::retain)
                .then();

        return session.send(outbound)
                .and(inbound)
                .doFinally(sig -> {
                    registry.remove(sessionId);
                    log.debug("ws closed session={} channel={} sig={}", sessionId, channel, sig);
                });
    }
}
