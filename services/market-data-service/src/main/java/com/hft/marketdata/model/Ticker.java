package com.hft.marketdata.model;

import java.math.BigDecimal;

/** Rolling 24h ticker for a symbol. */
public record Ticker(
        String symbol,
        BigDecimal lastPrice,
        BigDecimal high24h,
        BigDecimal low24h,
        BigDecimal volume24h,
        BigDecimal priceChange24h,
        BigDecimal priceChangePercent24h,
        long timestampMs
) {}
