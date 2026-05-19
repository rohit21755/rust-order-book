package com.hft.auth.service;

import com.hft.auth.config.AuthProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;

@Service
@RequiredArgsConstructor
public class RateLimitService {

    private final ReactiveStringRedisTemplate redis;
    private final AuthProperties props;

    /**
     * Sliding-window rate limit using Redis ZSET.
     * Key: ratelimit:{userId}
     * Adds current epoch-ms as both score+member, prunes < (now - window), counts members.
     * Returns true if under limit.
     */
    public Mono<Boolean> allow(String userId) {
        String key = "ratelimit:" + userId;
        long now = Instant.now().toEpochMilli();
        int windowSec = props.getRateLimit().getWindowSeconds();
        long cutoff = now - windowSec * 1000L;
        int limit = props.getRateLimit().getRequestsPerMinute();

        return redis.opsForZSet().removeRangeByScore(key, org.springframework.data.domain.Range.closed(0d, (double) cutoff))
                .then(redis.opsForZSet().add(key, String.valueOf(now), now))
                .then(redis.expire(key, Duration.ofSeconds(windowSec + 5)))
                .then(redis.opsForZSet().size(key))
                .map(count -> count != null && count <= limit);
    }
}
