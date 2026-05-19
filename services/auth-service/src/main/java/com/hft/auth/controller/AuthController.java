package com.hft.auth.controller;

import com.hft.auth.dto.LoginRequest;
import com.hft.auth.dto.RefreshRequest;
import com.hft.auth.dto.RegisterRequest;
import com.hft.auth.dto.TokenResponse;
import com.hft.auth.dto.UserDto;
import com.hft.auth.mapper.UserMapper;
import com.hft.auth.security.AuthenticatedUser;
import com.hft.auth.service.AuthService;
import com.hft.auth.service.UserService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseCookie;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.UUID;

@RestController
@RequestMapping("/auth")
@RequiredArgsConstructor
public class AuthController {

    private static final String REFRESH_COOKIE = "refresh_token";

    private final AuthService authService;
    private final UserService userService;
    private final UserMapper mapper;

    @PostMapping("/register")
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<ResponseEntity<TokenResponse>> register(@Valid @RequestBody RegisterRequest req) {
        return authService.register(req.email(), req.password(), req.role())
                .map(this::withRefreshCookie);
    }

    @PostMapping("/login")
    public Mono<ResponseEntity<TokenResponse>> login(@Valid @RequestBody LoginRequest req) {
        return authService.login(req.email(), req.password())
                .map(this::withRefreshCookie);
    }

    @PostMapping("/refresh")
    public Mono<TokenResponse> refresh(@RequestBody(required = false) RefreshRequest body,
                                       ServerWebExchange exchange) {
        String token = body != null && body.refreshToken() != null
                ? body.refreshToken()
                : cookie(exchange);
        return authService.refresh(token);
    }

    @PostMapping("/logout")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<ResponseEntity<Void>> logout(@RequestBody(required = false) RefreshRequest body,
                                             ServerWebExchange exchange) {
        String token = body != null && body.refreshToken() != null
                ? body.refreshToken()
                : cookie(exchange);
        return authService.logout(token)
                .thenReturn(ResponseEntity.noContent()
                        .header("Set-Cookie", clearedCookie().toString())
                        .<Void>build());
    }

    @GetMapping("/me")
    public Mono<UserDto> me() {
        return ReactiveSecurityContextHolder.getContext()
                .map(ctx -> (AuthenticatedUser) ctx.getAuthentication())
                .map(AuthenticatedUser::getUserId)
                .flatMap(this::loadUser);
    }

    private Mono<UserDto> loadUser(UUID id) {
        return userService.findById(id).map(mapper::toDto);
    }

    private ResponseEntity<TokenResponse> withRefreshCookie(TokenResponse tr) {
        ResponseCookie cookie = ResponseCookie.from(REFRESH_COOKIE, tr.refreshToken() == null ? "" : tr.refreshToken())
                .httpOnly(true)
                .secure(true)
                .sameSite("Strict")
                .path("/auth")
                .maxAge(Duration.ofDays(7))
                .build();
        return ResponseEntity.ok()
                .header("Set-Cookie", cookie.toString())
                .body(tr);
    }

    private ResponseCookie clearedCookie() {
        return ResponseCookie.from(REFRESH_COOKIE, "")
                .httpOnly(true).secure(true).sameSite("Strict")
                .path("/auth").maxAge(Duration.ZERO).build();
    }

    private String cookie(ServerWebExchange exchange) {
        var c = exchange.getRequest().getCookies().getFirst(REFRESH_COOKIE);
        return c == null ? null : c.getValue();
    }
}
