package com.hft.portfolio.dto;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

public record PortfolioDto(
        UUID userId,
        List<HoldingDto> holdings,
        BigDecimal totalValue,
        PnlSummary pnl
) {}
