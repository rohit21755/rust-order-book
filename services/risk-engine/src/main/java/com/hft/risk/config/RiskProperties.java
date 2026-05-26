package com.hft.risk.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.math.BigDecimal;

/** Bootstrap defaults + non-hot-reload knobs (JWT, Kafka topics, downstream URLs). */
@Data
@Configuration
@ConfigurationProperties(prefix = "risk")
public class RiskProperties {
    private Jwt jwt = new Jwt();
    private Kafka kafka = new Kafka();
    private Cache cache = new Cache();
    private Downstream downstream = new Downstream();
    private Parameters parameters = new Parameters();

    @Data public static class Jwt { private String secret; private String issuer = "hft-auth-service"; }
    @Data public static class Kafka { private String ordersTopic = "orders"; private String riskEventsTopic = "risk-events"; }
    @Data public static class Cache { private int balanceTtlSeconds = 5; private int priceTtlSeconds = 5; private int dailyPnlTtlSeconds = 5; }
    @Data public static class Downstream {
        private String portfolioUrl = "http://localhost:8084";
        private String marketDataUrl = "http://localhost:8083";
        private int timeoutMs = 1500;
        /** Long-lived service JWT used as Bearer for internal calls. */
        private String serviceJwt = "";
    }
    /** Hot-reloadable risk parameters (snapshot loaded into {@link RiskParametersHolder} at startup). */
    @Data public static class Parameters {
        private BigDecimal maxLeverage = new BigDecimal("10");
        private BigDecimal maxPositionSize = new BigDecimal("100000");
        private BigDecimal maxOrderValue = new BigDecimal("50000");
        private BigDecimal maxDailyLoss = new BigDecimal("-10000");
        private int abnormalActivityThreshold = 100;
        private int abnormalActivityWindowSeconds = 60;
        private BigDecimal priceDeviationPct = new BigDecimal("0.05");
    }
}
