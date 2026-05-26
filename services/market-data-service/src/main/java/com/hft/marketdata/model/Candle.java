package com.hft.marketdata.model;

import java.math.BigDecimal;

/** OHLCV candle. */
public record Candle(
        String symbol,
        String interval,
        long openTimeMs,
        BigDecimal open,
        BigDecimal high,
        BigDecimal low,
        BigDecimal close,
        BigDecimal volume,
        int tradeCount
) {}
