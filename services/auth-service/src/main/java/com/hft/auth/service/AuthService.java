package com.hft.auth.service;

import com.hft.auth.dto.TokenResponse;
import com.hft.auth.exception.AuthException;
import com.hft.auth.model.User;
import com.hft.auth.security.JwtUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;

@Service
@RequiredArgsConstructor
public class AuthService {

    private final UserService userService;
    private final RefreshTokenService refreshService;
    private final JwtUtil jwtUtil;

    public Mono<TokenResponse> register(String email, String password, String role) {
        return userService.register(email, password, role).flatMap(this::issueTokens);
    }

    public Mono<TokenResponse> login(String email, String password) {
        return userService.authenticate(email, password).flatMap(this::issueTokens);
    }

    public Mono<TokenResponse> refresh(String compoundRefresh) {
        if (compoundRefresh == null || compoundRefresh.isBlank()) {
            return Mono.error(AuthException.unauthorized("Missing refresh token"));
        }
        return refreshService.validate(compoundRefresh)
                .switchIfEmpty(Mono.error(AuthException.unauthorized("Invalid refresh token")))
                .flatMap(userService::findById)
                .flatMap(this::issueAccessOnly);
    }

    public Mono<Void> logout(String compoundRefresh) {
        if (compoundRefresh == null || compoundRefresh.isBlank()) return Mono.empty();
        return refreshService.revoke(compoundRefresh).then();
    }

    private Mono<TokenResponse> issueTokens(User u) {
        String access = jwtUtil.generateAccessToken(u.getId(), List.of(u.getRole()));
        return refreshService.issueAndIndex(u.getId())
                .map(refresh -> TokenResponse.bearer(access, refresh, jwtUtil.accessTtlSeconds()));
    }

    private Mono<TokenResponse> issueAccessOnly(User u) {
        String access = jwtUtil.generateAccessToken(u.getId(), List.of(u.getRole()));
        return Mono.just(TokenResponse.bearer(access, null, jwtUtil.accessTtlSeconds()));
    }
}
