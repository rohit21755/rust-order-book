package com.hft.kafka.event;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record OrderbookUpdateEvent(
        String symbol,
        long sequence,
        Type updateType,
        List<Level> bids,
        List<Level> asks,
        Instant timestamp
) implements EventEnvelope {

    public enum Type { SNAPSHOT, DELTA }

    public record Level(BigDecimal price, BigDecimal quantity) {}

    @Override
    public String partitionKey() {
        return symbol;
    }
}
