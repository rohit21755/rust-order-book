package com.hft.auth.repository;

import com.hft.auth.model.ApiKey;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Repository
public interface ApiKeyRepository extends ReactiveCrudRepository<ApiKey, UUID> {
    Mono<ApiKey> findByKeyHash(String keyHash);
    Flux<ApiKey> findByUserId(UUID userId);

    @Query("UPDATE api_keys SET revoked = TRUE WHERE id = :id AND user_id = :userId")
    Mono<Integer> revoke(UUID id, UUID userId);

    @Query("UPDATE api_keys SET last_used_at = NOW() WHERE id = :id")
    Mono<Integer> touch(UUID id);
}
