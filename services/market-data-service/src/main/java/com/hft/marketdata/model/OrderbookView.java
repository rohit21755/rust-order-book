package com.hft.marketdata.model;

import java.math.BigDecimal;
import java.util.List;

/** Orderbook snapshot pushed over WS / returned via REST. */
public record OrderbookView(
        String symbol,
        long sequence,
        List<Level> bids,
        List<Level> asks,
        long timestampMs
) {
    public record Level(BigDecimal price, BigDecimal quantity) {}
}
