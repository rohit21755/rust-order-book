package com.hft.portfolio.dto;

import java.math.BigDecimal;

public record PnlSummary(
        BigDecimal realizedPnl,
        BigDecimal unrealizedPnl,
        BigDecimal totalPnl
) {}
