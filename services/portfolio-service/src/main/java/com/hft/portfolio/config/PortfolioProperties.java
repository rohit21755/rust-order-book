package com.hft.portfolio.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "portfolio")
public class PortfolioProperties {

    private Jwt jwt = new Jwt();
    private Kafka kafka = new Kafka();
    private Cache cache = new Cache();
    private MarketData marketData = new MarketData();
    private Settlement settlement = new Settlement();

    @Data
    public static class Jwt {
        private String secret;
        private String issuer = "hft-auth-service";
    }

    @Data
    public static class Kafka {
        private String tradesTopic = "trades";
        private String portfolioEventsTopic = "portfolio-events";
    }

    @Data
    public static class Cache {
        private int portfolioTtlSeconds = 10;
        private long marketPriceTtlMillis = 2000;
    }

    @Data
    public static class MarketData {
        private String baseUrl = "http://localhost:8083";
        private int timeoutMs = 1500;
    }

    @Data
    public static class Settlement {
        private int optimisticRetryMax = 5;
    }
}
