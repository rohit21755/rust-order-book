package com.hft.marketdata.fanout;

import com.hft.marketdata.model.StreamType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Hot publishers per (stream, key). One Kafka consumer emits into a sink; N WebSocket
 * clients subscribe to the shared Flux (fan-out).
 *
 * <p>Per-client backpressure is applied at the subscriber boundary (see WS handlers):
 * each client wraps the shared Flux with {@code onBackpressureBuffer(bufferSize, DROP_OLDEST)}
 * so a slow client never blocks the publisher. The sink itself uses
 * {@link Sinks.MulticastSpec#directBestEffort()} — emissions never block; if there are no
 * subscribers or a subscriber cannot keep up at the sink level, the event is dropped for that
 * subscriber only.
 */
@Slf4j
@Component
public class MarketDataFanout {

    private final ConcurrentHashMap<String, Sinks.Many<Object>> sinks = new ConcurrentHashMap<>();

    private static String key(StreamType type, String symbolOrUser) {
        return type.name() + ":" + symbolOrUser;
    }

    private Sinks.Many<Object> sink(String key) {
        return sinks.computeIfAbsent(key, k -> Sinks.many().multicast().directBestEffort());
    }

    /** Emit a payload to all subscribers of (type, key). Non-blocking. */
    public void emit(StreamType type, String symbolOrUser, Object payload) {
        Sinks.EmitResult r = sink(key(type, symbolOrUser)).tryEmitNext(payload);
        if (r.isFailure() && r != Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER) {
            log.debug("emit failed type={} key={} result={}", type, symbolOrUser, r);
        }
    }

    /** Shared hot stream for (type, key). Multiple clients share one upstream. */
    public Flux<Object> stream(StreamType type, String symbolOrUser) {
        return sink(key(type, symbolOrUser)).asFlux();
    }

    /** Number of live sinks (diagnostics). */
    public int sinkCount() {
        return sinks.size();
    }
}
