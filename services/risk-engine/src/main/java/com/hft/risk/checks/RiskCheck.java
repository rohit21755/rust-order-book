package com.hft.risk.checks;

import java.util.Optional;

/**
 * One pure (no I/O) risk validation rule. Returns empty on PASS, a reason string on FAIL.
 * Implementations are stateless beans.
 */
public interface RiskCheck {
    /** Stable code used as metrics tag + RiskEvent reason. */
    String code();

    Optional<String> evaluate(RiskContext ctx);
}
