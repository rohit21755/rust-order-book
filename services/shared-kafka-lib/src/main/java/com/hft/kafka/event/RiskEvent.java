package com.hft.kafka.event;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record RiskEvent(
        Type eventType,
        Severity severity,
        UUID userId,
        UUID orderId,
        String symbol,
        BigDecimal exposureNotional,
        String reason,
        Instant timestamp
) implements EventEnvelope {

    public enum Type { LIMIT_BREACH, VAR_ALERT, KILL_SWITCH, RECOVERY }
    public enum Severity { INFO, WARN, CRITICAL }

    @Override
    public String partitionKey() {
        return userId != null ? userId.toString() : (symbol != null ? symbol : "global");
    }
}
