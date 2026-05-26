package com.hft.portfolio.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;

/** Wire format from the Rust engine `trades` topic (camelCase, decimal-as-string). */
public record TradeMessage(
        @JsonProperty("tradeId") String tradeId,
        @JsonProperty("buyOrderId") String buyOrderId,
        @JsonProperty("sellOrderId") String sellOrderId,
        @JsonProperty("buyerUserId") String buyerUserId,
        @JsonProperty("sellerUserId") String sellerUserId,
        @JsonProperty("symbol") String symbol,
        @JsonProperty("price") BigDecimal price,
        @JsonProperty("quantity") BigDecimal quantity,
        @JsonProperty("executedAtMs") long executedAtMs,
        @JsonProperty("sequence") long sequence
) {}
