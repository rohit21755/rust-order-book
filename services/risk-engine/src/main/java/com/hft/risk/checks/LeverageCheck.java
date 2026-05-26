package com.hft.risk.checks;

import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Optional;

/** LEVERAGE: orderValue / accountBalance <= maxLeverage. */
@Component
public class LeverageCheck implements RiskCheck {
    @Override public String code() { return "MAX_LEVERAGE"; }

    @Override
    public Optional<String> evaluate(RiskContext ctx) {
        BigDecimal balance = ctx.accountBalance();
        BigDecimal v = ctx.orderValue();
        if (balance == null || balance.signum() <= 0) {
            return Optional.of("zero balance — cannot leverage");
        }
        BigDecimal ratio = v.divide(balance, 8, RoundingMode.HALF_UP);
        if (ratio.compareTo(ctx.params().maxLeverage()) > 0) {
            return Optional.of("leverage " + ratio + " > max " + ctx.params().maxLeverage());
        }
        return Optional.empty();
    }
}
