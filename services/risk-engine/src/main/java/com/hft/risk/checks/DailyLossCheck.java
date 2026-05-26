package com.hft.risk.checks;

import org.springframework.stereotype.Component;

import java.util.Optional;

/** DAILY_LOSS: halt trading when dailyPnl < maxDailyLoss (negative threshold). */
@Component
public class DailyLossCheck implements RiskCheck {
    @Override public String code() { return "MAX_DAILY_LOSS"; }

    @Override
    public Optional<String> evaluate(RiskContext ctx) {
        if (ctx.dailyPnl().compareTo(ctx.params().maxDailyLoss()) < 0) {
            return Optional.of("daily pnl " + ctx.dailyPnl() + " < limit " + ctx.params().maxDailyLoss());
        }
        return Optional.empty();
    }
}
