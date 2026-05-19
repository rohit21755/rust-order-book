package com.hft.order.dto;

import java.math.BigDecimal;
import java.util.List;

public record OrderbookSnapshot(
        String symbol,
        long sequence,
        List<Level> bids,
        List<Level> asks
) {
    public record Level(BigDecimal price, BigDecimal quantity) {}
}
