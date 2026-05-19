package com.hft.shared.security;

import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.server.authentication.ServerAuthenticationConverter;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

public class BearerAuthenticationConverter implements ServerAuthenticationConverter {
    private static final String BEARER = "Bearer ";

    @Override
    public Mono<Authentication> convert(ServerWebExchange exchange) {
        String header = exchange.getRequest().getHeaders().getFirst("Authorization");
        if (header == null || !header.startsWith(BEARER)) return Mono.empty();
        String token = header.substring(BEARER.length()).trim();
        if (token.isEmpty()) return Mono.empty();
        return Mono.just(new UsernamePasswordAuthenticationToken(token, token));
    }
}
