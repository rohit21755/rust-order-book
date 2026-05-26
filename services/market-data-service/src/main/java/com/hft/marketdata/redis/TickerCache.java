package com.hft.marketdata.redis;

import com.hft.marketdata.config.MarketDataProperties;
import com.hft.marketdata.model.Ticker;
import com.hft.marketdata.ws.JsonCodec;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;

/** Caches the current ticker per symbol. Key: ticker:{symbol}, TTL 5s. */
@Slf4j
@Service
@RequiredArgsConstructor
public class TickerCache {

    private final ReactiveStringRedisTemplate redis;
    private final MarketDataProperties props;
    private final JsonCodec codec;

    public Mono<Boolean> put(Ticker ticker) {
        Duration ttl = Duration.ofSeconds(props.getCache().getTickerTtlSeconds());
        return redis.opsForValue().set("ticker:" + ticker.symbol(), codec.toJson(ticker), ttl);
    }

    public Mono<Ticker> get(String symbol) {
        return redis.opsForValue().get("ticker:" + symbol)
                .flatMap(json -> {
                    try {
                        return Mono.just(codec.mapper().readValue(json, Ticker.class));
                    } catch (Exception e) {
                        return Mono.empty();
                    }
                });
    }
}
