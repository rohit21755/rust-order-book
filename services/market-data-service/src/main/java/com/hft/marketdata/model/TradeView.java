package com.hft.marketdata.model;

import java.math.BigDecimal;

/** Public trade view (no user identity leaked). */
public record TradeView(
        String tradeId,
        String symbol,
        BigDecimal price,
        BigDecimal quantity,
        long sequence,
        long executedAtMs
) {}
