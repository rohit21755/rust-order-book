package com.hft.auth.service;

import com.hft.auth.config.AuthProperties;
import com.hft.auth.exception.AuthException;
import com.hft.auth.mapper.UserMapper;
import com.hft.auth.model.ApiKey;
import com.hft.auth.repository.ApiKeyRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.OffsetDateTime;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ApiKeyServiceTest {

    private final ApiKeyRepository repo = mock(ApiKeyRepository.class);
    private final UserMapper mapper = mock(UserMapper.class);
    private final AuthProperties props = new AuthProperties();
    private ApiKeyService svc;

    @BeforeEach
    void setUp() {
        props.getApiKey().setPrefix("hft_");
        props.getApiKey().setLengthBytes(32);
        svc = new ApiKeyService(repo, props, mapper);
    }

    @Test
    void createReturnsPlaintextOnce() {
        UUID userId = UUID.randomUUID();
        when(repo.save(any(ApiKey.class))).thenAnswer(inv -> {
            ApiKey k = inv.getArgument(0);
            k.setId(UUID.randomUUID());
            k.setCreatedAt(OffsetDateTime.now());
            return Mono.just(k);
        });
        when(mapper.toApiKeyResponseWithSecret(any(), any()))
                .thenAnswer(inv -> new com.hft.auth.dto.ApiKeyResponse(
                        UUID.randomUUID(), "hft_pre", null, inv.getArgument(1, String.class),
                        false, OffsetDateTime.now(), null));

        StepVerifier.create(svc.create(userId, "default"))
                .assertNext(resp -> {
                    assert resp.apiKey().startsWith("hft_");
                    assert resp.apiKey().length() > 16;
                })
                .verifyComplete();
    }

    @Test
    void revokeReturnsErrorWhenMissing() {
        UUID userId = UUID.randomUUID();
        UUID keyId = UUID.randomUUID();
        when(repo.revoke(keyId, userId)).thenReturn(Mono.just(0));

        StepVerifier.create(svc.revoke(userId, keyId))
                .expectErrorMatches(t -> t instanceof AuthException ax
                        && ax.getStatus().value() == 404)
                .verify();
    }

    @Test
    void revokeSucceedsWhenRowChanged() {
        UUID userId = UUID.randomUUID();
        UUID keyId = UUID.randomUUID();
        when(repo.revoke(keyId, userId)).thenReturn(Mono.just(1));

        StepVerifier.create(svc.revoke(userId, keyId)).verifyComplete();
    }

    @Test
    void resolveReturnsUserIdForActiveKey() {
        UUID userId = UUID.randomUUID();
        UUID keyId = UUID.randomUUID();
        ApiKey k = ApiKey.builder().id(keyId).userId(userId).revoked(false).build();
        when(repo.findByKeyHash(any())).thenReturn(Mono.just(k));
        when(repo.touch(keyId)).thenReturn(Mono.just(1));

        StepVerifier.create(svc.resolve("hft_plain"))
                .expectNext(userId).verifyComplete();
    }

    @Test
    void resolveSkipsRevokedKey() {
        ApiKey k = ApiKey.builder().userId(UUID.randomUUID()).revoked(true).build();
        when(repo.findByKeyHash(any())).thenReturn(Mono.just(k));

        StepVerifier.create(svc.resolve("hft_plain")).verifyComplete();
    }
}
