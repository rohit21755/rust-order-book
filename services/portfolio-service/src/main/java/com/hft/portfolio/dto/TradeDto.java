package com.hft.portfolio.dto;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record TradeDto(
        String tradeId,
        String symbol,
        String side,
        BigDecimal price,
        BigDecimal quantity,
        BigDecimal realizedPnl,
        OffsetDateTime executedAt
) {}
