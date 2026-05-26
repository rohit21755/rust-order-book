package com.hft.risk.service;

import com.hft.risk.config.RiskProperties;
import com.hft.risk.redis.CacheStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;

/** Market last-price (Redis cache TTL 5s; on miss → market-data REST). */
@Slf4j
@Service
public class MarketPriceClient {

    private final WebClient webClient;
    private final CacheStore cache;
    private final RiskProperties props;

    public MarketPriceClient(WebClient.Builder builder, CacheStore cache, RiskProperties props) {
        this.webClient = builder.baseUrl(props.getDownstream().getMarketDataUrl()).build();
        this.cache = cache;
        this.props = props;
    }

    private String key(String symbol) { return "risk:price:" + symbol; }

    public Mono<BigDecimal> lastPrice(String symbol) {
        return cache.getDecimal(key(symbol))
                .switchIfEmpty(fetch(symbol)
                        .flatMap(p -> cache.putDecimal(key(symbol), p,
                                Duration.ofSeconds(props.getCache().getPriceTtlSeconds())).thenReturn(p)));
    }

    private Mono<BigDecimal> fetch(String symbol) {
        return webClient.get().uri("/api/market/ticker/{symbol}", symbol)
                .retrieve()
                .bodyToMono(TickerView.class)
                .timeout(Duration.ofMillis(props.getDownstream().getTimeoutMs()))
                .map(TickerView::lastPrice)
                .onErrorResume(e -> {
                    log.warn("market price fetch failed for {}: {}", symbol, e.toString());
                    return Mono.empty();
                });
    }

    private record TickerView(String symbol, BigDecimal lastPrice) {}
}
