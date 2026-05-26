package com.hft.risk.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.util.UUID;

/** Wire format from `orders` topic (Order Service publishes shared-kafka-lib OrderEvent JSON). */
public record OrderMessage(
        @JsonProperty("eventType") String eventType,
        @JsonProperty("orderId") UUID orderId,
        @JsonProperty("userId") UUID userId,
        @JsonProperty("symbol") String symbol,
        @JsonProperty("side") String side,
        @JsonProperty("orderType") String orderType,
        @JsonProperty("price") BigDecimal price,
        @JsonProperty("stopPrice") BigDecimal stopPrice,
        @JsonProperty("quantity") BigDecimal quantity,
        @JsonProperty("idempotencyKey") String idempotencyKey
) {}
