package com.hft.portfolio.service;

import com.hft.portfolio.config.PortfolioProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fetches last price from Market Data Service REST, cached in-memory with a short TTL
 * to bound load. Non-blocking (WebClient). Missing price → Mono.empty (caller treats as 0 PnL).
 */
@Slf4j
@Service
public class MarketPriceClient {

    private final WebClient webClient;
    private final PortfolioProperties props;
    private final ConcurrentHashMap<String, Cached> cache = new ConcurrentHashMap<>();

    private record Cached(BigDecimal price, long expiresAt) {}

    public MarketPriceClient(WebClient.Builder builder, PortfolioProperties props) {
        this.props = props;
        this.webClient = builder.baseUrl(props.getMarketData().getBaseUrl()).build();
    }

    public Mono<BigDecimal> lastPrice(String symbol) {
        Cached c = cache.get(symbol);
        long now = System.currentTimeMillis();
        if (c != null && c.expiresAt() > now) {
            return Mono.just(c.price());
        }
        return webClient.get()
                .uri("/api/market/ticker/{symbol}", symbol)
                .retrieve()
                .bodyToMono(TickerResponse.class)
                .timeout(Duration.ofMillis(props.getMarketData().getTimeoutMs()))
                .retryWhen(Retry.backoff(1, Duration.ofMillis(100)))
                .map(TickerResponse::lastPrice)
                .doOnNext(p -> cache.put(symbol,
                        new Cached(p, now + props.getCache().getMarketPriceTtlMillis())))
                .onErrorResume(e -> {
                    log.warn("market price fetch failed for {}: {}", symbol, e.toString());
                    return c != null ? Mono.just(c.price()) : Mono.empty();
                });
    }

    private record TickerResponse(String symbol, BigDecimal lastPrice) {}
}
