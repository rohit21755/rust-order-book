# Graceful degradation

How each service behaves when a downstream is unavailable.

## Risk Engine down

**Order Service** treats risk as a hard gate by default.

| Config (order-service) | Behaviour on Risk Engine down |
|---|---|
| `hft.risk.on-down=halt` *(default)* | Submit returns 503; new orders rejected. Trading halted. |
| `hft.risk.on-down=bypass` | Submit proceeds without risk check, marks order with `risk-bypassed=true` header. |

Implementation: a Resilience4j circuit breaker wraps the inter-service risk call.
When the breaker is OPEN, the configured fallback path runs. Bypass is **opt-in only**
— production-sim default is `halt`.

## Matching Engine down

Orders sit safely in the **`orders` Kafka topic**. The Order Service publishes with
`acks=all` + idempotent producer, so the publish succeeds while the engine is offline.

Recovery:
1. Engine restarts, reads its Redis snapshot (`orderbook:{symbol}`), restores top-N
   levels.
2. Kafka consumer resumes from last committed offset.
3. Each replay event also passes through `processed_events`-style dedup keys in the
   engine, so re-processed records produce identical outputs.

No data loss. Latency spikes during catch-up; downstream services (portfolio, risk)
continue at-least-once on their consumers.

## Market Data Service down

WebSocket clients still receive **last-known orderbook + ticker** from the local Redis
snapshot. Each frame carries:

```json
{ "type": "orderbook", "stale": true, "lastUpdateMs": 1716120000000, ... }
```

Clients SHOULD ignore stale frames after 10s or display a "stale data" UI banner.

The REST endpoints answer from gRPC primary with Redis fallback (already implemented
in Order Service via `shared-grpc-lib`'s circuit breaker + `RedisOrderbookFallback`).

## Circuit breakers (Resilience4j)

Already wired in `shared-grpc-lib`'s `MatchingEngineGrpcClient` for the
order → matching engine query path:

| Knob | Default |
|---|---|
| `hft.grpc.circuit-breaker.failure-rate-threshold` | 50% |
| `hft.grpc.circuit-breaker.sliding-window-size` | 20 |
| `hft.grpc.circuit-breaker.wait-duration-in-open-state-ms` | 5000 |
| `hft.grpc.circuit-breaker.slow-call-duration-threshold-ms` | 50 |

To add a breaker for risk calls in your service:

```java
@Bean
CircuitBreaker riskCircuitBreaker() {
    return CircuitBreaker.of("risk-engine", CircuitBreakerConfig.custom()
        .failureRateThreshold(50f)
        .slidingWindowSize(20)
        .waitDurationInOpenState(Duration.ofSeconds(5))
        .build());
}

// inside the call
return riskClient.evaluate(req)
    .transformDeferred(CircuitBreakerOperator.of(riskCircuitBreaker))
    .onErrorResume(ex -> haltOrBypass());
```
