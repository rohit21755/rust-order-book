package com.hft.auth.service;

import com.hft.auth.config.AuthProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.ReactiveZSetOperations;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RateLimitServiceTest {

    @SuppressWarnings("unchecked")
    private final ReactiveStringRedisTemplate redis = mock(ReactiveStringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    private final ReactiveZSetOperations<String, String> zset = mock(ReactiveZSetOperations.class);
    private final AuthProperties props = new AuthProperties();

    private RateLimitService svc;

    @BeforeEach
    void setUp() {
        props.getRateLimit().setRequestsPerMinute(100);
        props.getRateLimit().setWindowSeconds(60);
        svc = new RateLimitService(redis, props);
        when(redis.opsForZSet()).thenReturn(zset);
        when(zset.removeRangeByScore(anyString(), any(Range.class))).thenReturn(Mono.just(0L));
        when(zset.add(anyString(), anyString(), anyDouble())).thenReturn(Mono.just(true));
        when(redis.expire(anyString(), any(Duration.class))).thenReturn(Mono.just(true));
    }

    @Test
    void allowsUnderLimit() {
        when(zset.size(eq("ratelimit:u1"))).thenReturn(Mono.just(42L));
        StepVerifier.create(svc.allow("u1")).expectNext(true).verifyComplete();
    }

    @Test
    void rejectsOverLimit() {
        when(zset.size(eq("ratelimit:u1"))).thenReturn(Mono.just(101L));
        StepVerifier.create(svc.allow("u1")).expectNext(false).verifyComplete();
    }

    @Test
    void allowsAtExactlyLimit() {
        when(zset.size(eq("ratelimit:u1"))).thenReturn(Mono.just(100L));
        StepVerifier.create(svc.allow("u1")).expectNext(true).verifyComplete();
    }
}
