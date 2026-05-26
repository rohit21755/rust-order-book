package com.hft.risk.checks;

import com.hft.risk.config.RiskParametersHolder.RiskParameters;
import com.hft.risk.kafka.OrderMessage;

import java.math.BigDecimal;

/**
 * Snapshot fed into each {@link RiskCheck}. Contains the order + pre-fetched downstream
 * values so checks never await I/O (they ran upstream once per order).
 */
public record RiskContext(
        OrderMessage order,
        RiskParameters params,
        BigDecimal accountBalance,
        BigDecimal currentPositionValue,
        BigDecimal marketPrice,
        BigDecimal dailyPnl,
        long recentOrderCount
) {
    /** Order notional value = price * quantity (MARKET uses marketPrice). */
    public BigDecimal orderValue() {
        BigDecimal px = order.price();
        if (px == null || px.signum() <= 0) px = marketPrice;
        if (px == null) return BigDecimal.ZERO;
        return px.multiply(order.quantity());
    }
}
