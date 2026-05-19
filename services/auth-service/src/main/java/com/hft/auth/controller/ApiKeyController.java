package com.hft.auth.controller;

import com.hft.auth.dto.ApiKeyCreateRequest;
import com.hft.auth.dto.ApiKeyResponse;
import com.hft.auth.security.AuthenticatedUser;
import com.hft.auth.service.ApiKeyService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

@RestController
@RequestMapping("/auth/api-keys")
@RequiredArgsConstructor
public class ApiKeyController {

    private final ApiKeyService apiKeyService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<ApiKeyResponse> create(@Valid @RequestBody(required = false) ApiKeyCreateRequest req) {
        String label = req == null ? null : req.label();
        return currentUserId().flatMap(uid -> apiKeyService.create(uid, label));
    }

    @GetMapping
    public Flux<ApiKeyResponse> list() {
        return currentUserId().flatMapMany(apiKeyService::list);
    }

    @DeleteMapping("/{keyId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> revoke(@PathVariable UUID keyId) {
        return currentUserId().flatMap(uid -> apiKeyService.revoke(uid, keyId));
    }

    private Mono<UUID> currentUserId() {
        return ReactiveSecurityContextHolder.getContext()
                .map(ctx -> (AuthenticatedUser) ctx.getAuthentication())
                .map(AuthenticatedUser::getUserId);
    }
}
