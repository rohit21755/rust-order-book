package com.hft.auth.security;

import com.hft.auth.service.ApiKeyService;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.security.core.context.SecurityContextImpl;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.util.List;

/** Auths via X-API-Key header BEFORE Spring Security AuthN filter consumes the request. */
@Component
@RequiredArgsConstructor
public class ApiKeyAuthenticationFilter implements WebFilter {

    private static final String HEADER = "X-API-Key";
    private final ApiKeyService apiKeyService;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String key = exchange.getRequest().getHeaders().getFirst(HEADER);
        if (key == null || key.isBlank()) {
            return chain.filter(exchange);
        }
        return apiKeyService.resolve(key)
                .map(userId -> new AuthenticatedUser(userId, key,
                        List.of(new SimpleGrantedAuthority("ROLE_API_KEY"))))
                .flatMap(auth -> chain.filter(exchange)
                        .contextWrite(ReactiveSecurityContextHolder.withSecurityContext(
                                Mono.just(new SecurityContextImpl(auth)))))
                .switchIfEmpty(chain.filter(exchange));
    }
}
