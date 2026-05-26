package com.hft.kafka.event;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record TradeEvent(
        UUID tradeId,
        UUID buyOrderId,
        UUID sellOrderId,
        UUID buyerUserId,
        UUID sellerUserId,
        String symbol,
        BigDecimal price,
        BigDecimal quantity,
        Instant executedAt
) implements EventEnvelope {

    @Override
    public String partitionKey() {
        return symbol;
    }
}
