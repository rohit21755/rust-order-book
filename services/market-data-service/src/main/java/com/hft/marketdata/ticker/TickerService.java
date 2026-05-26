package com.hft.marketdata.ticker;

import com.hft.marketdata.model.Ticker;
import com.hft.marketdata.model.TradeMessage;
import com.hft.marketdata.redis.TickerCache;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Maintains rolling 24h ticker stats per symbol in-memory; mirrors current ticker to Redis.
 *
 * 24h window kept as a deque of (timestampMs, price, qty); pruned on each update. Memory bounded
 * by trade rate × 24h — acceptable for a simulation. For production this would move to ClickHouse
 * aggregation, but the spec wants live ticker stats here.
 */
@Service
@RequiredArgsConstructor
public class TickerService {

    private static final long DAY_MS = 24 * 60 * 60 * 1000L;

    private final TickerCache cache;
    private final ConcurrentHashMap<String, Window> windows = new ConcurrentHashMap<>();

    private record Tick(long ts, BigDecimal price, BigDecimal qty) {}

    private static final class Window {
        final ConcurrentLinkedDeque<Tick> ticks = new ConcurrentLinkedDeque<>();
        volatile BigDecimal lastPrice = BigDecimal.ZERO;
    }

    /** Update stats with a new trade; returns the recomputed ticker (also cached). */
    public Mono<Ticker> onTrade(TradeMessage t) {
        Window w = windows.computeIfAbsent(t.symbol(), k -> new Window());
        long now = t.executedAtMs() > 0 ? t.executedAtMs() : System.currentTimeMillis();
        w.ticks.addLast(new Tick(now, t.price(), t.quantity()));
        w.lastPrice = t.price();
        prune(w, now - DAY_MS);
        Ticker ticker = compute(t.symbol(), w, now);
        return cache.put(ticker).thenReturn(ticker);
    }

    /** Current ticker: Redis cache → in-memory recompute fallback. */
    public Mono<Ticker> current(String symbol) {
        return cache.get(symbol).switchIfEmpty(Mono.fromSupplier(() -> {
            Window w = windows.get(symbol);
            long now = System.currentTimeMillis();
            if (w == null) {
                return new Ticker(symbol, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO,
                        BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, now);
            }
            prune(w, now - DAY_MS);
            return compute(symbol, w, now);
        }));
    }

    private void prune(Window w, long cutoff) {
        Tick head;
        while ((head = w.ticks.peekFirst()) != null && head.ts() < cutoff) {
            w.ticks.pollFirst();
        }
    }

    private Ticker compute(String symbol, Window w, long now) {
        BigDecimal high = null, low = null, vol = BigDecimal.ZERO, first = null, last = w.lastPrice;
        for (Tick tk : w.ticks) {
            if (first == null) first = tk.price();
            if (high == null || tk.price().compareTo(high) > 0) high = tk.price();
            if (low == null || tk.price().compareTo(low) < 0) low = tk.price();
            vol = vol.add(tk.qty());
            last = tk.price();
        }
        if (high == null) high = last;
        if (low == null) low = last;
        if (first == null) first = last;

        BigDecimal change = last.subtract(first);
        BigDecimal changePct = first.signum() == 0
                ? BigDecimal.ZERO
                : change.divide(first, 8, RoundingMode.HALF_UP).multiply(BigDecimal.valueOf(100));

        return new Ticker(symbol, last, high, low, vol, change, changePct, now);
    }
}
