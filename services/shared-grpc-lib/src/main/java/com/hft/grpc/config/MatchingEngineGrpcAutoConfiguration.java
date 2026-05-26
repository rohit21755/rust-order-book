package com.hft.grpc.config;

import com.hft.grpc.client.MatchingEngineGrpcClient;
import com.hft.grpc.fallback.OrderbookFallback;
import com.hft.matching.grpc.MatchingEngineServiceGrpc;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import reactor.core.publisher.Mono;

import java.time.Duration;

/** Auto-wires the gRPC channel pool, circuit breaker, and reactive client. */
@Slf4j
@AutoConfiguration
@ConditionalOnClass(MatchingEngineServiceGrpc.class)
@EnableConfigurationProperties(GrpcClientProperties.class)
public class MatchingEngineGrpcAutoConfiguration {

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public GrpcChannelPool grpcChannelPool(GrpcClientProperties props) {
        return new GrpcChannelPool(props);
    }

    @Bean
    @ConditionalOnMissingBean(name = "matchingEngineCircuitBreaker")
    public CircuitBreaker matchingEngineCircuitBreaker(GrpcClientProperties props) {
        var cb = props.getCircuitBreaker();
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(cb.getFailureRateThreshold())
                .slidingWindowSize(cb.getSlidingWindowSize())
                .minimumNumberOfCalls(cb.getMinimumNumberOfCalls())
                .waitDurationInOpenState(Duration.ofMillis(cb.getWaitDurationInOpenStateMs()))
                .permittedNumberOfCallsInHalfOpenState(cb.getPermittedCallsInHalfOpen())
                .slowCallDurationThreshold(Duration.ofMillis(cb.getSlowCallDurationThresholdMs()))
                .slowCallRateThreshold(cb.getSlowCallRateThreshold())
                .build();
        CircuitBreaker breaker = CircuitBreaker.of("matching-engine", config);
        breaker.getEventPublisher()
                .onStateTransition(e -> log.warn("matching-engine CB state: {}", e.getStateTransition()));
        return breaker;
    }

    /** Default fallback: errors out. Services should override with a Redis-backed bean. */
    @Bean
    @ConditionalOnMissingBean
    public OrderbookFallback orderbookFallback() {
        return (symbol, depth) -> Mono.error(
                new IllegalStateException("matching engine unavailable and no fallback configured for " + symbol));
    }

    @Bean
    @ConditionalOnMissingBean
    public MatchingEngineGrpcClient matchingEngineGrpcClient(
            GrpcChannelPool pool,
            GrpcClientProperties props,
            CircuitBreaker matchingEngineCircuitBreaker,
            OrderbookFallback fallback) {
        return new MatchingEngineGrpcClient(pool, props, matchingEngineCircuitBreaker, fallback);
    }
}
