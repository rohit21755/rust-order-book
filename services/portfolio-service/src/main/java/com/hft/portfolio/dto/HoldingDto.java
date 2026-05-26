package com.hft.portfolio.dto;

import java.math.BigDecimal;

public record HoldingDto(
        String symbol,
        BigDecimal quantity,
        BigDecimal avgBuyPrice,
        BigDecimal currentPrice,
        BigDecimal currentValue,
        BigDecimal unrealizedPnl
) {}
