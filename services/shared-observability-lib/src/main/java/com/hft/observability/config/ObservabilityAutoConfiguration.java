package com.hft.observability.config;

import com.hft.observability.tracing.Tracing;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Auto-wires the {@link Tracing} helper for any service that depends on shared-observability-lib.
 *
 * <p>The OpenTelemetry SDK itself is wired by {@code opentelemetry-spring-boot-starter} which
 * the lib pulls transitively; this autoconfig only adds the manual-span helper.
 */
@AutoConfiguration
@ConditionalOnClass(OpenTelemetry.class)
@EnableConfigurationProperties(ObservabilityProperties.class)
public class ObservabilityAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public OpenTelemetry openTelemetry() {
        return GlobalOpenTelemetry.get();
    }

    @Bean
    @ConditionalOnMissingBean
    public Tracing tracing(OpenTelemetry otel, ObservabilityProperties props) {
        return new Tracing(otel, props.getEnvironment());
    }
}
