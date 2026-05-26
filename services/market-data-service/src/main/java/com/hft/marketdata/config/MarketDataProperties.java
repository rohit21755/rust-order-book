package com.hft.marketdata.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Data
@Configuration
@ConfigurationProperties(prefix = "market")
public class MarketDataProperties {

    private List<String> supportedSymbols = List.of();
    private Jwt jwt = new Jwt();
    private Ws ws = new Ws();
    private Cache cache = new Cache();
    private Clickhouse clickhouse = new Clickhouse();
    private Kafka kafka = new Kafka();

    @Data
    public static class Jwt {
        private String secret;
        private String issuer = "hft-auth-service";
    }

    @Data
    public static class Ws {
        /** Per-client outbound buffer before drop-oldest. */
        private int clientBufferSize = 1000;
        /** Heartbeat ping interval (ms). */
        private long heartbeatIntervalMs = 30_000;
        /** Missed pongs before drop. */
        private int maxMissedPongs = 2;
        /** Orderbook push cadence (ms). */
        private long orderbookPushMs = 100;
        /** Ticker push cadence (ms). */
        private long tickerPushMs = 1000;
        /** Orderbook depth pushed over WS. */
        private int orderbookDepth = 10;
    }

    @Data
    public static class Cache {
        private int tickerTtlSeconds = 5;
        private int recentTradesTtlSeconds = 60;
        private int recentTradesMax = 50;
    }

    @Data
    public static class Clickhouse {
        private String url = "http://localhost:8123";
        private String database = "default";
        private String user = "default";
        private String password = "";
        private int batchMaxRecords = 100;
        private long batchMaxIntervalMs = 1000;
        private int queryTimeoutMs = 5000;
    }

    @Data
    public static class Kafka {
        private String tradesTopic = "trades";
        private String orderbookTopic = "orderbook-updates";
        private String portfolioTopic = "portfolio-events";
    }
}
