package com.hft.portfolio.dto;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record PnlHistoryPoint(
        String symbol,
        BigDecimal realizedPnl,
        BigDecimal unrealizedPnl,
        OffsetDateTime timestamp
) {}
