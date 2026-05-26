package com.hft.marketdata.redis;

import com.hft.marketdata.config.MarketDataProperties;
import com.hft.marketdata.model.TradeView;
import com.hft.marketdata.ws.JsonCodec;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

/** Caches recent N trades per symbol in a capped Redis list. Key: trades:{symbol}, TTL 60s. */
@Service
@RequiredArgsConstructor
public class RecentTradesCache {

    private final ReactiveStringRedisTemplate redis;
    private final MarketDataProperties props;
    private final JsonCodec codec;

    private String key(String symbol) {
        return "trades:" + symbol;
    }

    /** Push newest trade, trim to max, refresh TTL. */
    public Mono<Void> push(TradeView trade) {
        String key = key(trade.symbol());
        int max = props.getCache().getRecentTradesMax();
        Duration ttl = Duration.ofSeconds(props.getCache().getRecentTradesTtlSeconds());
        return redis.opsForList().leftPush(key, codec.toJson(trade))
                .then(redis.opsForList().trim(key, 0, max - 1))
                .then(redis.expire(key, ttl))
                .then();
    }

    /** Read newest-first up to `limit`. */
    public Flux<TradeView> recent(String symbol, int limit) {
        return redis.opsForList().range(key(symbol), 0, limit - 1)
                .flatMap(json -> {
                    try {
                        return Mono.just(codec.mapper().readValue(json, TradeView.class));
                    } catch (Exception e) {
                        return Mono.empty();
                    }
                });
    }
}
