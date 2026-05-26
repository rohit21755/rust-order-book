package com.hft.kafka.event;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.math.BigDecimal;
import java.time.Instant;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record MarketDataEvent(
        Type eventType,
        String symbol,
        BigDecimal bidPrice,
        BigDecimal askPrice,
        BigDecimal lastPrice,
        BigDecimal volume24h,
        Instant timestamp
) implements EventEnvelope {

    public enum Type { TICK, TRADE_PRINT, BBO }

    @Override
    public String partitionKey() {
        return symbol;
    }
}
