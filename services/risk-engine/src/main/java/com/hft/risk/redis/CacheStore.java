package com.hft.risk.redis;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;

/** Thin TTL'd string store used by balance / price / daily-PnL caches. */
@Service
@RequiredArgsConstructor
public class CacheStore {

    private final ReactiveStringRedisTemplate redis;

    public Mono<BigDecimal> getDecimal(String key) {
        return redis.opsForValue().get(key).map(BigDecimal::new);
    }

    public Mono<Boolean> putDecimal(String key, BigDecimal value, Duration ttl) {
        return redis.opsForValue().set(key, value.toPlainString(), ttl);
    }
}
