package com.hft.risk.service;

import com.hft.risk.config.RiskProperties;
import com.hft.risk.redis.CacheStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

/** Daily realized+unrealized PnL via Portfolio Service. Redis-cached (TTL 5s). */
@Slf4j
@Service
public class DailyPnlClient {

    private final WebClient webClient;
    private final CacheStore cache;
    private final RiskProperties props;

    public DailyPnlClient(WebClient.Builder builder, CacheStore cache, RiskProperties props) {
        this.webClient = builder.baseUrl(props.getDownstream().getPortfolioUrl()).build();
        this.cache = cache;
        this.props = props;
    }

    private String key(UUID userId) { return "risk:dailypnl:" + userId; }

    public Mono<BigDecimal> dailyPnl(UUID userId) {
        return cache.getDecimal(key(userId))
                .switchIfEmpty(fetch(userId)
                        .flatMap(p -> cache.putDecimal(key(userId), p,
                                Duration.ofSeconds(props.getCache().getDailyPnlTtlSeconds())).thenReturn(p)));
    }

    private Mono<BigDecimal> fetch(UUID userId) {
        OffsetDateTime from = OffsetDateTime.now(ZoneOffset.UTC).toLocalDate()
                .atStartOfDay().atOffset(ZoneOffset.UTC);
        OffsetDateTime to = OffsetDateTime.now(ZoneOffset.UTC);
        return webClient.get()
                .uri(b -> b.path("/api/portfolio/pnl/history")
                        .queryParam("from", from.toString())
                        .queryParam("to", to.toString())
                        .build())
                .header("Authorization", "Bearer " + props.getDownstream().getServiceJwt())
                .retrieve()
                .bodyToFlux(PnlPoint.class)
                .map(p -> nonNull(p.realizedPnl()).add(nonNull(p.unrealizedPnl())))
                .reduce(BigDecimal.ZERO, BigDecimal::add)
                .timeout(Duration.ofMillis(props.getDownstream().getTimeoutMs()))
                .onErrorResume(e -> {
                    log.warn("daily pnl fetch failed for {}: {}", userId, e.toString());
                    return Mono.just(BigDecimal.ZERO);
                });
    }

    private static BigDecimal nonNull(BigDecimal v) { return v == null ? BigDecimal.ZERO : v; }

    private record PnlPoint(String symbol, BigDecimal realizedPnl, BigDecimal unrealizedPnl, String timestamp) {}
}
