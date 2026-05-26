package com.hft.portfolio.service;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class PnlCalculatorTest {

    private final PnlCalculator calc = new PnlCalculator();

    @Test
    void weightedAvgFirstBuy() {
        BigDecimal avg = calc.weightedAvg(new BigDecimal("0"), new BigDecimal("0"),
                new BigDecimal("2"), new BigDecimal("100"));
        assertThat(avg).isEqualByComparingTo("100");
    }

    @Test
    void weightedAvgSecondBuyBlends() {
        // hold 2@100, buy 2@200 → avg 150
        BigDecimal avg = calc.weightedAvg(new BigDecimal("2"), new BigDecimal("100"),
                new BigDecimal("2"), new BigDecimal("200"));
        assertThat(avg).isEqualByComparingTo("150");
    }

    @Test
    void realizedOnSellPositive() {
        // sell 1 @ 120, avg 100 → +20
        BigDecimal r = calc.realizedOnSell(new BigDecimal("120"), new BigDecimal("100"), new BigDecimal("1"));
        assertThat(r).isEqualByComparingTo("20");
    }

    @Test
    void realizedOnSellNegative() {
        BigDecimal r = calc.realizedOnSell(new BigDecimal("80"), new BigDecimal("100"), new BigDecimal("2"));
        assertThat(r).isEqualByComparingTo("-40");
    }

    @Test
    void unrealizedNullPriceIsZero() {
        BigDecimal u = calc.unrealized(null, new BigDecimal("100"), new BigDecimal("1"));
        assertThat(u).isEqualByComparingTo("0");
    }

    @Test
    void unrealizedComputed() {
        BigDecimal u = calc.unrealized(new BigDecimal("110"), new BigDecimal("100"), new BigDecimal("3"));
        assertThat(u).isEqualByComparingTo("30");
    }
}
