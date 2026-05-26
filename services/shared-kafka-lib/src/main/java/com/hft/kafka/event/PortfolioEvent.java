package com.hft.kafka.event;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record PortfolioEvent(
        Type eventType,
        UUID userId,
        String symbol,
        BigDecimal holdingQuantity,
        BigDecimal avgPrice,
        BigDecimal realizedPnl,
        BigDecimal unrealizedPnl,
        Instant timestamp
) implements EventEnvelope {

    public enum Type { POSITION_UPDATE, PNL_SNAPSHOT, MARGIN_CALL }

    @Override
    public String partitionKey() {
        return userId.toString();
    }
}
