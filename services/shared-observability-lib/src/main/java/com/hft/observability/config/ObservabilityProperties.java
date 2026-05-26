package com.hft.observability.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * `hft.observability.*` knobs.
 *
 * <p>OTel SDK is configured by `opentelemetry-spring-boot-starter` via standard
 * {@code otel.*} environment variables / Spring properties (e.g. `otel.exporter.otlp.endpoint`,
 * `otel.traces.sampler`). These extras are HFT-specific.
 */
@Data
@ConfigurationProperties(prefix = "hft.observability")
public class ObservabilityProperties {
    /** Logical environment tag attached to every span as `deployment.environment`. */
    private String environment = "dev";
    /** Override service.name reported to OTel (defaults to spring.application.name). */
    private String serviceName;
    /** Toggle to add Kafka header tracing propagation in shared-kafka-lib. */
    private boolean kafkaTracingEnabled = true;
}
