package com.hft.risk.checks;

import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Optional;

/** PRICE_DEVIATION: only applies to LIMIT orders; |orderPrice - marketPrice| / marketPrice <= pct. */
@Component
public class PriceDeviationCheck implements RiskCheck {
    @Override public String code() { return "PRICE_DEVIATION"; }

    @Override
    public Optional<String> evaluate(RiskContext ctx) {
        String type = ctx.order().orderType();
        if (type == null || !"LIMIT".equalsIgnoreCase(type)) return Optional.empty();
        BigDecimal mkt = ctx.marketPrice();
        BigDecimal px = ctx.order().price();
        if (mkt == null || mkt.signum() <= 0 || px == null) return Optional.empty();
        BigDecimal dev = px.subtract(mkt).abs().divide(mkt, 8, RoundingMode.HALF_UP);
        if (dev.compareTo(ctx.params().priceDeviationPct()) > 0) {
            return Optional.of("price deviation " + dev + " > limit " + ctx.params().priceDeviationPct());
        }
        return Optional.empty();
    }
}
