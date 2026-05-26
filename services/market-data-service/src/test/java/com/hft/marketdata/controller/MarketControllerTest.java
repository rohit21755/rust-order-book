package com.hft.marketdata.controller;

import com.hft.grpc.client.MatchingEngineGrpcClient;
import com.hft.marketdata.candle.CandleQueryService;
import com.hft.marketdata.config.MarketDataProperties;
import com.hft.marketdata.model.Ticker;
import com.hft.marketdata.redis.RecentTradesCache;
import com.hft.marketdata.ticker.TickerService;
import com.hft.matching.grpc.OrderBookResponse;
import com.hft.matching.grpc.PriceLevel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MarketControllerTest {

    TickerService tickerService;
    CandleQueryService candleQuery;
    RecentTradesCache recentTrades;
    MatchingEngineGrpcClient engineClient;
    MarketDataProperties props;
    WebTestClient client;

    @BeforeEach
    void setUp() {
        tickerService = mock(TickerService.class);
        candleQuery = mock(CandleQueryService.class);
        recentTrades = mock(RecentTradesCache.class);
        engineClient = mock(MatchingEngineGrpcClient.class);
        props = new MarketDataProperties();
        props.setSupportedSymbols(List.of("BTC-USDT", "ETH-USDT"));
        client = WebTestClient.bindToController(
                new MarketController(tickerService, candleQuery, recentTrades, engineClient, props)).build();
    }

    @Test
    void tickerReturnsCurrent() {
        when(tickerService.current("BTC-USDT")).thenReturn(Mono.just(new Ticker(
                "BTC-USDT", new BigDecimal("100"), new BigDecimal("110"), new BigDecimal("90"),
                new BigDecimal("5"), new BigDecimal("1"), new BigDecimal("1"), 1L)));

        client.get().uri("/api/market/ticker/BTC-USDT")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.symbol").isEqualTo("BTC-USDT")
                .jsonPath("$.lastPrice").isEqualTo("100");
    }

    @Test
    void symbolsReturnsList() {
        client.get().uri("/api/market/symbols")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$[0]").isEqualTo("BTC-USDT")
                .jsonPath("$[1]").isEqualTo("ETH-USDT");
    }

    @Test
    void orderbookMapsGrpcResponse() {
        OrderBookResponse resp = OrderBookResponse.newBuilder()
                .setSymbol("BTC-USDT").setSequence(3).setTimestampMs(123)
                .addBids(PriceLevel.newBuilder().setPrice("100").setQuantity("1").setOrderCount(1))
                .addAsks(PriceLevel.newBuilder().setPrice("101").setQuantity("2").setOrderCount(1))
                .build();
        when(engineClient.getOrderBook(anyString(), anyInt())).thenReturn(Mono.just(resp));

        client.get().uri("/api/market/orderbook/BTC-USDT?depth=10")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.symbol").isEqualTo("BTC-USDT")
                .jsonPath("$.sequence").isEqualTo(3)
                .jsonPath("$.bids[0].price").isEqualTo("100")
                .jsonPath("$.asks[0].quantity").isEqualTo("2");
    }
}
