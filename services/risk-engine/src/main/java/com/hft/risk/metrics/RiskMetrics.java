package com.hft.risk.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@RequiredArgsConstructor
public class RiskMetrics {

    private final MeterRegistry registry;

    public void rejection(String reason) {
        Counter.builder("risk_rejections_total")
                .tag("reason", reason)
                .register(registry)
                .increment();
    }

    public void approval() {
        Counter.builder("risk_approvals_total").register(registry).increment();
    }

    public void recordDuration(Duration d) {
        Timer.builder("risk_checks_duration_seconds")
                .publishPercentileHistogram()
                .sla(Duration.ofMillis(1), Duration.ofMillis(5), Duration.ofMillis(10), Duration.ofMillis(50))
                .register(registry)
                .record(d);
    }
}
