package com.hft.marketdata.candle;

import com.hft.marketdata.clickhouse.ClickHouseGateway;
import com.hft.marketdata.model.Candle;
import com.hft.marketdata.model.TradeView;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.util.Set;

/** Read-side for historical candles + trades from ClickHouse. */
@Service
@RequiredArgsConstructor
public class CandleQueryService {

    private static final Set<String> VALID_INTERVALS = Set.of("1m", "5m", "15m", "1h", "1d");

    private final ClickHouseGateway gateway;

    public Flux<Candle> candles(String symbol, String interval, int limit, int offset) {
        if (!VALID_INTERVALS.contains(interval)) {
            return Flux.error(new IllegalArgumentException("invalid interval: " + interval));
        }
        int boundedLimit = Math.max(1, Math.min(limit, 1000));
        int boundedOffset = Math.max(0, offset);
        return gateway.queryCandles(symbol, interval, boundedLimit, boundedOffset);
    }

    public Flux<TradeView> recentTrades(String symbol, int limit) {
        int boundedLimit = Math.max(1, Math.min(limit, 500));
        return gateway.queryRecentTrades(symbol, boundedLimit);
    }
}
