package com.hft.auth.service;

import com.hft.auth.security.JwtUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class RefreshTokenService {

    private final ReactiveStringRedisTemplate redis;
    private final JwtUtil jwtUtil;

    private String key(UUID userId, String tokenId) {
        return "refresh:" + userId + ":" + tokenId;
    }

    /** parse compound "{tokenId}.{token}" + verify against Redis. */
    public Mono<UUID> validate(String compound) {
        String[] parts = parse(compound);
        if (parts == null) return Mono.empty();
        // We do not know userId — scan via pattern would be expensive. Use convention:
        // store reverse-lookup key as well: refresh-lookup:{tokenId} = "{userId}:{token}"
        String tokenId = parts[0];
        String token = parts[1];
        return redis.opsForValue().get("refresh-lookup:" + tokenId)
                .flatMap(v -> {
                    int idx = v.indexOf(':');
                    if (idx < 0) return Mono.empty();
                    String userId = v.substring(0, idx);
                    String stored = v.substring(idx + 1);
                    if (!stored.equals(token)) return Mono.empty();
                    return Mono.just(UUID.fromString(userId));
                });
    }

    /** Atomic issue: writes both primary + lookup keys. */
    public Mono<String> issueAndIndex(UUID userId) {
        String tokenId = UUID.randomUUID().toString();
        String token = jwtUtil.generateRefreshToken(userId);
        String compound = tokenId + "." + token;
        var ttl = jwtUtil.refreshTtl();
        return redis.opsForValue().set(key(userId, tokenId), token, ttl)
                .then(redis.opsForValue().set("refresh-lookup:" + tokenId, userId + ":" + token, ttl))
                .thenReturn(compound);
    }

    public Mono<Long> revoke(String compound) {
        String[] parts = parse(compound);
        if (parts == null) return Mono.just(0L);
        String tokenId = parts[0];
        return redis.opsForValue().get("refresh-lookup:" + tokenId)
                .flatMap(v -> {
                    int idx = v.indexOf(':');
                    if (idx < 0) return Mono.just(0L);
                    String userId = v.substring(0, idx);
                    return redis.delete(key(UUID.fromString(userId), tokenId), "refresh-lookup:" + tokenId);
                })
                .defaultIfEmpty(0L);
    }

    public Mono<Long> revokeAll(UUID userId) {
        String pattern = "refresh:" + userId + ":*";
        return redis.scan(org.springframework.data.redis.core.ScanOptions.scanOptions().match(pattern).build())
                .flatMap(redis::delete)
                .reduce(0L, Long::sum);
    }

    private String[] parse(String compound) {
        if (compound == null) return null;
        int idx = compound.indexOf('.');
        if (idx <= 0 || idx == compound.length() - 1) return null;
        return new String[]{compound.substring(0, idx), compound.substring(idx + 1)};
    }
}
