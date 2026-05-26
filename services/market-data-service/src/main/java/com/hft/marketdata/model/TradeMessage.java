package com.hft.marketdata.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;

/** Wire format published by the Rust engine to the `trades` topic (camelCase, decimal-as-string). */
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
) {
    public TradeView toView() {
        return new TradeView(tradeId, symbol, price, quantity, sequence, executedAtMs);
    }
}
