package com.hft.auth.security;

import com.hft.auth.service.RateLimitService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class RateLimitWebFilter implements WebFilter {

    private final RateLimitService rateLimitService;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        return ReactiveSecurityContextHolder.getContext()
                .map(ctx -> ctx.getAuthentication())
                .filter(a -> a != null && a.isAuthenticated() && a.getPrincipal() != null)
                .map(a -> a.getPrincipal().toString())
                .defaultIfEmpty(remoteIp(exchange))
                .flatMap(key -> rateLimitService.allow(key))
                .flatMap(ok -> {
                    if (ok) return chain.filter(exchange);
                    exchange.getResponse().setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
                    return exchange.getResponse().setComplete();
                });
    }

    private String remoteIp(ServerWebExchange exchange) {
        var addr = exchange.getRequest().getRemoteAddress();
        return addr == null ? "anonymous" : addr.getAddress().getHostAddress();
    }
}
