package com.hft.risk.dto;

import java.math.BigDecimal;
import java.util.UUID;

public record UserRiskStatus(
        UUID userId,
        boolean halted,
        BigDecimal balance,
        BigDecimal dailyPnl,
        long recentOrderCount
) {}
