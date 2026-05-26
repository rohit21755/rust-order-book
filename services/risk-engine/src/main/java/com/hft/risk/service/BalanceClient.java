package com.hft.risk.service;

import com.hft.risk.config.RiskProperties;
import com.hft.risk.redis.CacheStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * User "balance" approximation: sum of currentValue across holdings + assumed cash.
 * In a real system cash sits on a wallet endpoint; here we sum holding values from the
 * Portfolio Service. Cached in Redis (TTL 5s) to keep the hot path under 10ms.
 */
@Slf4j
@Service
public class BalanceClient {

    private final WebClient webClient;
    private final CacheStore cache;
    private final RiskProperties props;

    public BalanceClient(WebClient.Builder builder, CacheStore cache, RiskProperties props) {
        this.webClient = builder.baseUrl(props.getDownstream().getPortfolioUrl()).build();
        this.cache = cache;
        this.props = props;
    }

    private String key(UUID userId) { return "risk:balance:" + userId; }

    public Mono<BigDecimal> balance(UUID userId) {
        return cache.getDecimal(key(userId))
                .switchIfEmpty(fetch(userId)
                        .flatMap(b -> cache.putDecimal(key(userId), b,
                                Duration.ofSeconds(props.getCache().getBalanceTtlSeconds())).thenReturn(b)));
    }

    private Mono<BigDecimal> fetch(UUID userId) {
        return webClient.get()
                .uri("/api/portfolio/holdings")
                .header("Authorization", "Bearer " + props.getDownstream().getServiceJwt())
                .retrieve()
                .bodyToFlux(HoldingView.class)
                .map(h -> h.currentValue() == null ? BigDecimal.ZERO : h.currentValue())
                .reduce(BigDecimal.ZERO, BigDecimal::add)
                .timeout(Duration.ofMillis(props.getDownstream().getTimeoutMs()))
                .onErrorResume(e -> {
                    log.warn("balance fetch failed for {}: {}", userId, e.toString());
                    return Mono.just(BigDecimal.ZERO);
                });
    }

    private record HoldingView(String symbol, BigDecimal quantity, BigDecimal avgBuyPrice,
                               BigDecimal currentPrice, BigDecimal currentValue, BigDecimal unrealizedPnl) {
        @SuppressWarnings("unused") List<Object> _unused() { return List.of(); }
    }
}
