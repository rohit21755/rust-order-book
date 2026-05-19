package com.hft.auth.service;

import com.hft.auth.config.AuthProperties;
import com.hft.auth.exception.AuthException;
import com.hft.auth.mapper.UserMapper;
import com.hft.auth.dto.ApiKeyResponse;
import com.hft.auth.model.ApiKey;
import com.hft.auth.repository.ApiKeyRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class ApiKeyService {

    private final ApiKeyRepository repo;
    private final AuthProperties props;
    private final UserMapper mapper;
    private final SecureRandom secureRandom = new SecureRandom();

    /**
     * Generates plain key (shown once), persists hash. Returns DTO with plaintext populated.
     */
    public Mono<ApiKeyResponse> create(UUID userId, String label) {
        byte[] buf = new byte[props.getApiKey().getLengthBytes()];
        secureRandom.nextBytes(buf);
        String secret = Base64.getUrlEncoder().withoutPadding().encodeToString(buf);
        String plain = props.getApiKey().getPrefix() + secret;
        String prefix = plain.substring(0, Math.min(plain.length(), 12));

        // hash on bounded elastic to avoid blocking the netty event loop
        return Mono.fromCallable(() -> sha256(plain))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(hash -> {
                    ApiKey entity = ApiKey.builder()
                            .userId(userId)
                            .keyHash(hash)
                            .keyPrefix(prefix)
                            .label(label)
                            .revoked(false)
                            .createdAt(OffsetDateTime.now())
                            .build();
                    return repo.save(entity);
                })
                .map(saved -> mapper.toApiKeyResponseWithSecret(saved, plain));
    }

    public Mono<Void> revoke(UUID userId, UUID keyId) {
        return repo.revoke(keyId, userId)
                .flatMap(rows -> rows == 0
                        ? Mono.error(AuthException.notFound("API key not found"))
                        : Mono.empty());
    }

    public Flux<ApiKeyResponse> list(UUID userId) {
        return repo.findByUserId(userId).map(mapper::toApiKeyResponse);
    }

    /** Lookup by plaintext (hash compare). Returns owning userId or empty. */
    public Mono<UUID> resolve(String plainKey) {
        return Mono.fromCallable(() -> sha256(plainKey))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(repo::findByKeyHash)
                .filter(k -> !k.isRevoked())
                .flatMap(k -> repo.touch(k.getId()).thenReturn(k.getUserId()));
    }

    static String sha256(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] out = md.digest(input.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(out);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
