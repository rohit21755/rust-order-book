package com.hft.risk.redis;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.UUID;

/** Set of halted user ids in Redis (key: risk:halted). */
@Service
@RequiredArgsConstructor
public class HaltRegistry {

    private static final String KEY = "risk:halted";

    private final ReactiveStringRedisTemplate redis;

    public Mono<Boolean> halt(UUID userId) {
        return redis.opsForSet().add(KEY, userId.toString()).map(n -> n != null && n > 0);
    }

    public Mono<Boolean> resume(UUID userId) {
        return redis.opsForSet().remove(KEY, userId.toString()).map(n -> n != null && n > 0);
    }

    public Mono<Boolean> isHalted(UUID userId) {
        return redis.opsForSet().isMember(KEY, userId.toString()).map(Boolean.TRUE::equals);
    }
}
