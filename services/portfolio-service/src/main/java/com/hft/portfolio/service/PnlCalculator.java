package com.hft.portfolio.service;

import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

/**
 * Pure Decimal PnL math. No floating point anywhere.
 */
@Component
public class PnlCalculator {

    /** 8-dp precision matches the DECIMAL(28,8) columns. */
    private static final MathContext MC = new MathContext(34, RoundingMode.HALF_UP);
    private static final int SCALE = 8;

    /**
     * Weighted-average buy price after adding {@code addQty} at {@code addPrice}.
     * newAvg = (oldQty*oldAvg + addQty*addPrice) / (oldQty + addQty)
     */
    public BigDecimal weightedAvg(BigDecimal oldQty, BigDecimal oldAvg, BigDecimal addQty, BigDecimal addPrice) {
        BigDecimal newQty = oldQty.add(addQty);
        if (newQty.signum() == 0) return BigDecimal.ZERO.setScale(SCALE, RoundingMode.HALF_UP);
        BigDecimal numerator = oldQty.multiply(oldAvg, MC).add(addQty.multiply(addPrice, MC), MC);
        return numerator.divide(newQty, SCALE, RoundingMode.HALF_UP);
    }

    /** Realized PnL on a sell: (sellPrice - avgBuyPrice) * quantity. */
    public BigDecimal realizedOnSell(BigDecimal sellPrice, BigDecimal avgBuyPrice, BigDecimal quantity) {
        return sellPrice.subtract(avgBuyPrice).multiply(quantity, MC).setScale(SCALE, RoundingMode.HALF_UP);
    }

    /** Unrealized PnL: (currentPrice - avgBuyPrice) * quantity. */
    public BigDecimal unrealized(BigDecimal currentPrice, BigDecimal avgBuyPrice, BigDecimal quantity) {
        if (currentPrice == null) return BigDecimal.ZERO.setScale(SCALE, RoundingMode.HALF_UP);
        return currentPrice.subtract(avgBuyPrice).multiply(quantity, MC).setScale(SCALE, RoundingMode.HALF_UP);
    }

    /** Position value: currentPrice * quantity. */
    public BigDecimal value(BigDecimal currentPrice, BigDecimal quantity) {
        if (currentPrice == null) return BigDecimal.ZERO.setScale(SCALE, RoundingMode.HALF_UP);
        return currentPrice.multiply(quantity, MC).setScale(SCALE, RoundingMode.HALF_UP);
    }
}
