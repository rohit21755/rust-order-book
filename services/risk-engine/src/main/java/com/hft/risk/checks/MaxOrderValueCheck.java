package com.hft.risk.checks;

import org.springframework.stereotype.Component;

import java.util.Optional;

/** MAX_ORDER_VALUE: single-order notional cap. */
@Component
public class MaxOrderValueCheck implements RiskCheck {
    @Override public String code() { return "MAX_ORDER_VALUE"; }

    @Override
    public Optional<String> evaluate(RiskContext ctx) {
        var v = ctx.orderValue();
        if (v.compareTo(ctx.params().maxOrderValue()) > 0) {
            return Optional.of("order value " + v + " > max " + ctx.params().maxOrderValue());
        }
        return Optional.empty();
    }
}
