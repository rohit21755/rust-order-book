package com.hft.order.kafka;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Event payload sent to Kafka `orders` topic. JSON for now; protobuf-ready. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record OrderEvent(
        Type eventType,
        UUID orderId,
        UUID userId,
        String symbol,
        String side,
        String orderType,
        BigDecimal price,
        BigDecimal stopPrice,
        BigDecimal quantity,
        Instant timestamp,
        String idempotencyKey
) {
    public enum Type { NEW, CANCEL }
}
