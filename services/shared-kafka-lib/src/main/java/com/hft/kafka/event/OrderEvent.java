package com.hft.kafka.event;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record OrderEvent(
        Type eventType,
        UUID orderId,
        UUID userId,
        String symbol,
        Side side,
        OrderType orderType,
        BigDecimal price,
        BigDecimal stopPrice,
        BigDecimal quantity,
        String idempotencyKey,
        Instant timestamp
) implements EventEnvelope {

    public enum Type { NEW, CANCEL, MODIFY }
    public enum Side { BUY, SELL }
    public enum OrderType { LIMIT, MARKET, STOP_LOSS }

    @Override
    public String partitionKey() {
        return orderId != null ? orderId.toString() : symbol;
    }
}
