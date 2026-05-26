package com.hft.grpc.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Binds {@code hft.grpc.*}. */
@Data
@ConfigurationProperties(prefix = "hft.grpc")
public class GrpcClientProperties {

    /** Matching engine host. */
    private String host = "localhost";
    /** Matching engine gRPC port. */
    private int port = 50051;
    /** Enable TLS (dev: trust self-signed via trustCertCollection). */
    private boolean tlsEnabled = false;
    /** PEM trust cert path; null = system trust store. */
    private String trustCertPath;
    /** Override authority (SNI) for self-signed dev certs. */
    private String authorityOverride;

    /** Per-call deadline (ms). Spec target < 5ms; default 2000 generous for cold start. */
    private long deadlineMs = 2000;
    /** Keepalive ping interval (ms). */
    private long keepAliveMs = 30_000;
    /** Keepalive timeout (ms). */
    private long keepAliveTimeoutMs = 5_000;

    // Connection pooling knobs (gRPC multiplexes; pool = multiple channels).
    /** Number of channels in the round-robin pool. */
    private int poolSize = 4;
    /** Max inbound message size (bytes). */
    private int maxInboundMessageSize = 4 * 1024 * 1024;

    // Circuit breaker.
    private CircuitBreaker circuitBreaker = new CircuitBreaker();

    @Data
    public static class CircuitBreaker {
        private float failureRateThreshold = 50.0f;
        private int slidingWindowSize = 20;
        private int minimumNumberOfCalls = 10;
        private long waitDurationInOpenStateMs = 5_000;
        private int permittedCallsInHalfOpen = 3;
        private long slowCallDurationThresholdMs = 50;
        private float slowCallRateThreshold = 80.0f;
    }
}
