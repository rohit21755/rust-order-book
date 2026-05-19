package com.hft.order.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.math.BigDecimal;
import java.util.List;

@Data
@Configuration
@ConfigurationProperties(prefix = "order")
public class OrderProperties {

    private Jwt jwt = new Jwt();
    private List<String> supportedSymbols = List.of();
    private Limits limits = new Limits();
    private Cache cache = new Cache();
    private Kafka kafka = new Kafka();

    @Data
    public static class Jwt {
        private String secret;
        private String issuer = "hft-auth-service";
    }

    @Data
    public static class Limits {
        private int maxOpenOrdersPerSymbol = 10;
        private BigDecimal maxPositionQuantity = new BigDecimal("100000");
        private BigDecimal minQuantity = new BigDecimal("0.00000001");
    }

    @Data
    public static class Cache {
        private int orderTtlHours = 24;
        private int idempotencyTtlHours = 24;
    }

    @Data
    public static class Kafka {
        private String bootstrapServers = "localhost:29092";
        private String ordersTopic = "orders";
        private String dlqTopic = "orders.DLQ";
        private int publishRetries = 3;
        private int publishBackoffMillis = 100;
        private int requestTimeoutMillis = 5000;
        private int deliveryTimeoutMillis = 15000;
        private String acks = "all";
    }
}
