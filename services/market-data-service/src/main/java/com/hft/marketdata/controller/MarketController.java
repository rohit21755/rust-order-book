package com.hft.marketdata.controller;

import com.hft.grpc.client.MatchingEngineGrpcClient;
import com.hft.marketdata.candle.CandleQueryService;
import com.hft.marketdata.config.MarketDataProperties;
import com.hft.marketdata.model.Candle;
import com.hft.marketdata.model.OrderbookView;
import com.hft.marketdata.model.Ticker;
import com.hft.marketdata.model.TradeView;
import com.hft.marketdata.redis.RecentTradesCache;
import com.hft.marketdata.ticker.TickerService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.List;

@RestController
@RequestMapping("/api/market")
@RequiredArgsConstructor
public class MarketController {

    private final TickerService tickerService;
    private final CandleQueryService candleQuery;
    private final RecentTradesCache recentTrades;
    private final MatchingEngineGrpcClient engineClient;
    private final MarketDataProperties props;

    @GetMapping("/ticker/{symbol}")
    public Mono<Ticker> ticker(@PathVariable String symbol) {
        return tickerService.current(symbol);
    }

    @GetMapping("/candles/{symbol}")
    public Flux<Candle> candles(
            @PathVariable String symbol,
            @RequestParam(defaultValue = "1m") String interval,
            @RequestParam(defaultValue = "500") int limit,
            @RequestParam(defaultValue = "0") int offset) {
        return candleQuery.candles(symbol, interval, limit, offset);
    }

    @GetMapping("/trades/{symbol}")
    public Flux<TradeView> trades(
            @PathVariable String symbol,
            @RequestParam(defaultValue = "50") int limit) {
        // Hot path: Redis cache first, fall back to ClickHouse history.
        return recentTrades.recent(symbol, limit)
                .switchIfEmpty(candleQuery.recentTrades(symbol, limit));
    }

    @GetMapping("/orderbook/{symbol}")
    public Mono<OrderbookView> orderbook(
            @PathVariable String symbol,
            @RequestParam(defaultValue = "20") int depth) {
        int bounded = Math.max(1, Math.min(depth, 100));
        return engineClient.getOrderBook(symbol, bounded).map(r -> {
            List<OrderbookView.Level> bids = r.getBidsList().stream()
                    .map(l -> new OrderbookView.Level(new BigDecimal(l.getPrice()), new BigDecimal(l.getQuantity())))
                    .toList();
            List<OrderbookView.Level> asks = r.getAsksList().stream()
                    .map(l -> new OrderbookView.Level(new BigDecimal(l.getPrice()), new BigDecimal(l.getQuantity())))
                    .toList();
            return new OrderbookView(r.getSymbol(), r.getSequence(), bids, asks, r.getTimestampMs());
        });
    }

    @GetMapping("/symbols")
    public List<String> symbols() {
        return props.getSupportedSymbols();
    }
}
