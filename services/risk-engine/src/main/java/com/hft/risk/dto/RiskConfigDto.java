package com.hft.risk.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

public record RiskConfigDto(
        @NotNull @Positive BigDecimal maxLeverage,
        @NotNull @Positive BigDecimal maxPositionSize,
        @NotNull @Positive BigDecimal maxOrderValue,
        @NotNull BigDecimal maxDailyLoss,
        @NotNull @Positive Integer abnormalActivityThreshold,
        @NotNull @Positive Integer abnormalActivityWindowSeconds,
        @NotNull @Positive BigDecimal priceDeviationPct
) {}
