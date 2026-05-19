package com.hft.auth.service;

import com.hft.auth.config.AuthProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class SessionService {

    private final ReactiveStringRedisTemplate redis;
    private final AuthProperties props;

    private Duration ttl() {
        return Duration.ofMinutes(props.getSession().getTtlMinutes());
    }

    public Mono<String> create(UUID userId, String payload) {
        String sessionId = UUID.randomUUID().toString();
        return redis.opsForValue().set("session:" + sessionId, userId + "|" + payload, ttl())
                .thenReturn(sessionId);
    }

    public Mono<String> get(String sessionId) {
        return redis.opsForValue().get("session:" + sessionId);
    }

    public Mono<Boolean> touch(String sessionId) {
        return redis.expire("session:" + sessionId, ttl());
    }

    public Mono<Boolean> destroy(String sessionId) {
        return redis.delete("session:" + sessionId).map(n -> n > 0);
    }
}
