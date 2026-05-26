package com.hft.order.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Duration;

/** Order-service custom Prometheus metrics. */
@Component
@RequiredArgsConstructor
public class OrderMetrics {

    private final MeterRegistry registry;

    public void recordSubmission(String symbol, String type, String status) {
        Counter.builder("order_submissions_total")
                .tag("symbol", symbol)
                .tag("type", type)
                .tag("status", status)
                .register(registry)
                .increment();
    }

    public void recordStageLatency(String stage, Duration duration) {
        Timer.builder("order_processing_latency_seconds")
                .tag("stage", stage)
                .publishPercentileHistogram()
                .sla(Duration.ofMillis(1), Duration.ofMillis(10), Duration.ofMillis(50),
                        Duration.ofMillis(100), Duration.ofMillis(500))
                .register(registry)
                .record(duration);
    }
}
