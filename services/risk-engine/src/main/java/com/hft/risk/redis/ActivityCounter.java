package com.hft.risk.redis;

import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * Sliding-window order counter per user via Redis ZSET.
 * Key: activity:{userId}; score+member = epoch-ms. Prunes outside window, returns size.
 */
@Service
@RequiredArgsConstructor
public class ActivityCounter {

    private final ReactiveStringRedisTemplate redis;

    private String key(UUID userId) { return "activity:" + userId; }

    /** Record an order event + return total count inside [now-windowSec, now]. */
    public Mono<Long> recordAndCount(UUID userId, int windowSeconds) {
        long now = Instant.now().toEpochMilli();
        long cutoff = now - windowSeconds * 1000L;
        String k = key(userId);
        return redis.opsForZSet().removeRangeByScore(k, Range.closed(0d, (double) cutoff))
                .then(redis.opsForZSet().add(k, now + ":" + UUID.randomUUID(), now))
                .then(redis.expire(k, Duration.ofSeconds(windowSeconds + 5L)))
                .then(redis.opsForZSet().size(k))
                .map(n -> n == null ? 0L : n);
    }

    public Mono<Long> count(UUID userId, int windowSeconds) {
        long now = Instant.now().toEpochMilli();
        long cutoff = now - windowSeconds * 1000L;
        String k = key(userId);
        return redis.opsForZSet().removeRangeByScore(k, Range.closed(0d, (double) cutoff))
                .then(redis.opsForZSet().size(k))
                .map(n -> n == null ? 0L : n);
    }
}
