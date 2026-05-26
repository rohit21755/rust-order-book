package com.hft.risk.checks;

import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Optional;

/** EXPOSURE: currentPositionValue + newOrderValue <= maxPositionSize. */
@Component
public class ExposureCheck implements RiskCheck {
    @Override public String code() { return "MAX_POSITION_SIZE"; }

    @Override
    public Optional<String> evaluate(RiskContext ctx) {
        BigDecimal total = ctx.currentPositionValue().add(ctx.orderValue());
        if (total.compareTo(ctx.params().maxPositionSize()) > 0) {
            return Optional.of("position " + total + " > max " + ctx.params().maxPositionSize());
        }
        return Optional.empty();
    }
}
