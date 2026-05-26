package com.hft.marketdata.candle;

import com.hft.marketdata.clickhouse.ClickHouseGateway;
import com.hft.marketdata.model.Candle;
import com.hft.marketdata.model.TradeMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Aggregates trades into OHLCV candles for intervals 1m/5m/15m/1h/1d.
 *
 * Each trade updates the live bucket for every interval. A scheduled sweep seals buckets whose
 * window has closed and batch-writes them to ClickHouse (batched insert, ReplacingMergeTree dedups).
 */
@Slf4j
@Component
public class CandleAggregator {

    /** interval name → length in ms. */
    private static final Map<String, Long> INTERVALS = Map.of(
            "1m", 60_000L,
            "5m", 300_000L,
            "15m", 900_000L,
            "1h", 3_600_000L,
            "1d", 86_400_000L);

    private final ClickHouseGateway gateway;

    /** key: symbol|interval|bucketStart → mutable candle. */
    private final ConcurrentHashMap<String, Bucket> buckets = new ConcurrentHashMap<>();

    public CandleAggregator(ClickHouseGateway gateway) {
        this.gateway = gateway;
    }

    private static final class Bucket {
        final String symbol;
        final String interval;
        final long openTime;
        BigDecimal open, high, low, close;
        BigDecimal volume = BigDecimal.ZERO;
        int count = 0;

        Bucket(String symbol, String interval, long openTime, BigDecimal first) {
            this.symbol = symbol;
            this.interval = interval;
            this.openTime = openTime;
            this.open = first;
            this.high = first;
            this.low = first;
            this.close = first;
        }

        synchronized void apply(BigDecimal price, BigDecimal qty) {
            if (price.compareTo(high) > 0) high = price;
            if (price.compareTo(low) < 0) low = price;
            close = price;
            volume = volume.add(qty);
            count++;
        }

        Candle toCandle() {
            return new Candle(symbol, interval, openTime, open, high, low, close, volume, count);
        }
    }

    private static long bucketStart(long ts, long len) {
        return (ts / len) * len;
    }

    private static String key(String symbol, String interval, long start) {
        return symbol + '|' + interval + '|' + start;
    }

    /** Fold a trade into all interval buckets. */
    public void onTrade(TradeMessage t) {
        long ts = t.executedAtMs() > 0 ? t.executedAtMs() : System.currentTimeMillis();
        INTERVALS.forEach((interval, len) -> {
            long start = bucketStart(ts, len);
            String k = key(t.symbol(), interval, start);
            buckets.compute(k, (kk, existing) -> {
                Bucket b = existing != null ? existing : new Bucket(t.symbol(), interval, start, t.price());
                b.apply(t.price(), t.quantity());
                return b;
            });
        });
    }

    /** Seal + flush closed buckets. Runs every minute per spec; smaller intervals already closed. */
    @Scheduled(fixedDelayString = "${market.candle.flush-interval-ms:60000}")
    public void sealAndFlush() {
        long now = System.currentTimeMillis();
        List<Candle> due = new ArrayList<>();
        var it = buckets.entrySet().iterator();
        while (it.hasNext()) {
            var entry = it.next();
            Bucket b = entry.getValue();
            long len = INTERVALS.getOrDefault(b.interval, 60_000L);
            if (b.openTime + len <= now) {
                due.add(b.toCandle());
                it.remove();
            }
        }
        if (due.isEmpty()) return;
        gateway.insertCandles(due)
                .subscribe(
                        n -> log.debug("flushed {} candles", n),
                        err -> log.error("candle flush failed for {} candles", due.size(), err));
    }

    /** Visible for tests. */
    int liveBucketCount() {
        return buckets.size();
    }
}
