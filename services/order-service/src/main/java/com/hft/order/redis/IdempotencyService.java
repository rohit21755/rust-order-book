package com.hft.order.redis;

import com.hft.order.config.OrderProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.UUID;

/**
 * Reserves idempotency keys via SETNX (atomic). First caller gets true; duplicates get false.
 * Key shape: idem:{userId}:{key} → orderId. TTL 24h (configurable).
 */
@Service
@RequiredArgsConstructor
public class IdempotencyService {

    private final ReactiveStringRedisTemplate redis;
    private final OrderProperties props;

    public Mono<Boolean> reserve(UUID userId, String key, UUID orderId) {
        Duration ttl = Duration.ofHours(props.getCache().getIdempotencyTtlHours());
        return redis.opsForValue()
                .setIfAbsent("idem:" + userId + ":" + key, orderId.toString(), ttl);
    }

    public Mono<String> lookup(UUID userId, String key) {
        return redis.opsForValue().get("idem:" + userId + ":" + key);
    }

    public Mono<Long> release(UUID userId, String key) {
        return redis.delete("idem:" + userId + ":" + key);
    }
}
