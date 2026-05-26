package com.hft.risk.checks;

import org.springframework.stereotype.Component;

import java.util.Optional;

/** ABNORMAL_ACTIVITY: recent order count > threshold within window. */
@Component
public class AbnormalActivityCheck implements RiskCheck {
    @Override public String code() { return "ABNORMAL_ACTIVITY"; }

    @Override
    public Optional<String> evaluate(RiskContext ctx) {
        if (ctx.recentOrderCount() > ctx.params().abnormalActivityThreshold()) {
            return Optional.of("recent orders " + ctx.recentOrderCount() + " > limit "
                    + ctx.params().abnormalActivityThreshold()
                    + " in " + ctx.params().abnormalActivityWindowSeconds() + "s");
        }
        return Optional.empty();
    }
}
