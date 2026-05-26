package com.hft.marketdata.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.util.List;

/** Wire format published by the Rust engine to `orderbook-updates` (serde snake_case). */
public record OrderbookMessage(
        @JsonProperty("symbol") String symbol,
        @JsonProperty("sequence") long sequence,
        @JsonProperty("bids") List<Level> bids,
        @JsonProperty("asks") List<Level> asks,
        @JsonProperty("timestamp_ms") long timestampMs
) {
    public record Level(
            @JsonProperty("price") BigDecimal price,
            @JsonProperty("quantity") BigDecimal quantity
    ) {}

    public OrderbookView toView(int depth) {
        List<OrderbookView.Level> b = bids.stream().limit(depth)
                .map(l -> new OrderbookView.Level(l.price(), l.quantity())).toList();
        List<OrderbookView.Level> a = asks.stream().limit(depth)
                .map(l -> new OrderbookView.Level(l.price(), l.quantity())).toList();
        return new OrderbookView(symbol, sequence, b, a, timestampMs);
    }
}
