package com.hft.risk.checks;

import com.hft.risk.config.RiskParametersHolder.RiskParameters;
import com.hft.risk.kafka.OrderMessage;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class RiskCheckTests {

    private final RiskParameters params = new RiskParameters(
            new BigDecimal("10"), new BigDecimal("100000"), new BigDecimal("50000"),
            new BigDecimal("-10000"), 100, 60, new BigDecimal("0.05"));

    private OrderMessage order(String type, String price, String qty) {
        return new OrderMessage("NEW", UUID.randomUUID(), UUID.randomUUID(),
                "BTC-USDT", "BUY", type,
                price == null ? null : new BigDecimal(price), null, new BigDecimal(qty), "k");
    }

    private RiskContext ctx(OrderMessage o, String balance, String marketPrice, String pnl, long recent) {
        return new RiskContext(o, params, new BigDecimal(balance), BigDecimal.ZERO,
                new BigDecimal(marketPrice), new BigDecimal(pnl), recent);
    }

    @Test
    void maxOrderValueRejectsOver50k() {
        var c = new MaxOrderValueCheck();
        var result = c.evaluate(ctx(order("LIMIT", "100", "600"), "1000000", "100", "0", 0));
        assertThat(result).isPresent();
    }

    @Test
    void leverageRejectsOver10x() {
        var c = new LeverageCheck();
        // notional 100*200=20000, balance 1000 → 20x > 10
        var result = c.evaluate(ctx(order("LIMIT", "100", "200"), "1000", "100", "0", 0));
        assertThat(result).isPresent();
    }

    @Test
    void leverageZeroBalanceFails() {
        var c = new LeverageCheck();
        var result = c.evaluate(ctx(order("LIMIT", "100", "1"), "0", "100", "0", 0));
        assertThat(result).isPresent();
    }

    @Test
    void dailyLossRejectsBelowThreshold() {
        var c = new DailyLossCheck();
        var result = c.evaluate(ctx(order("LIMIT", "100", "1"), "100000", "100", "-15000", 0));
        assertThat(result).isPresent();
    }

    @Test
    void dailyLossPassesAboveThreshold() {
        var c = new DailyLossCheck();
        var result = c.evaluate(ctx(order("LIMIT", "100", "1"), "100000", "100", "-9000", 0));
        assertThat(result).isEmpty();
    }

    @Test
    void abnormalActivityRejectsOverThreshold() {
        var c = new AbnormalActivityCheck();
        var result = c.evaluate(ctx(order("LIMIT", "100", "1"), "100000", "100", "0", 150L));
        assertThat(result).isPresent();
    }

    @Test
    void priceDeviationRejectsOverPct() {
        var c = new PriceDeviationCheck();
        // limit at 110, market 100 → 10% > 5%
        var result = c.evaluate(ctx(order("LIMIT", "110", "1"), "100000", "100", "0", 0));
        assertThat(result).isPresent();
    }

    @Test
    void priceDeviationSkipsMarketOrders() {
        var c = new PriceDeviationCheck();
        var result = c.evaluate(ctx(order("MARKET", null, "1"), "100000", "100", "0", 0));
        assertThat(result).isEmpty();
    }

    @Test
    void exposurePassesWhenWithinLimit() {
        var c = new ExposureCheck();
        var result = c.evaluate(ctx(order("LIMIT", "100", "100"), "100000", "100", "0", 0));
        assertThat(result).isEmpty();
    }
}
