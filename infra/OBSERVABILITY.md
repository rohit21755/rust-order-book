# Observability stack

OpenTelemetry tracing → Jaeger. Micrometer + Prometheus metrics → Grafana dashboards.

## Compose

```bash
docker network create hft-network 2>/dev/null || true
docker compose -f docker-compose.yml -f infra/docker/observability.compose.yml up -d jaeger prometheus grafana
```

| URL | Service |
|---|---|
| http://localhost:16686 | Jaeger UI |
| http://localhost:9090  | Prometheus |
| http://localhost:3000  | Grafana (admin/admin) |

Grafana datasources + dashboards are provisioned automatically from `infra/grafana/`.

## Adding observability to a Java service

1. Depend on `shared-observability-lib`:

   ```xml
   <dependency>
       <groupId>com.hft</groupId>
       <artifactId>shared-observability-lib</artifactId>
       <version>0.1.0</version>
   </dependency>
   ```

2. Add `application.yml`:

   ```yaml
   management:
     endpoints:
       web:
         exposure:
           include: health,info,metrics,prometheus
     metrics:
       tags:
         service: <service-name>
         environment: ${HFT_ENV:dev}

   otel:
     service:
       name: <service-name>
     exporter:
       otlp:
         endpoint: ${OTEL_EXPORTER_OTLP_ENDPOINT:http://jaeger:4317}
         protocol: grpc
     traces:
       sampler: ${OTEL_TRACES_SAMPLER:always_on}     # prod sim: traceidratio
       sampler.arg: ${OTEL_TRACES_SAMPLER_ARG:1.0}   # prod sim: 0.01
     propagators: tracecontext,baggage

   hft:
     observability:
       environment: ${HFT_ENV:dev}
   ```

3. Auto-instrumentation covers HTTP (WebFlux + WebClient), Kafka (via shared-kafka-lib W3C propagation), R2DBC, Redis-reactive.

4. Manual spans via `Tracing` (auto-wired bean):

   ```java
   public Mono<OrderResponse> submit(OrderRequest req) {
       Attributes attrs = Attributes.of(
           Tracing.SYMBOL, req.symbol(),
           Tracing.STAGE, "validate"
       );
       return tracing.wrap("order.validate", attrs, span -> validator.run(req));
   }
   ```

## Sampling

- Dev: `OTEL_TRACES_SAMPLER=always_on` (100%) — also configured in `jaeger/sampling.json` for the collector.
- Prod sim: `OTEL_TRACES_SAMPLER=traceidratio`, `OTEL_TRACES_SAMPLER_ARG=0.01` (1%).

## Standard attribute keys (no PII)

- `hft.user.id` — user UUID only (no PII)
- `hft.order.id` — order UUID
- `hft.symbol` — trading symbol
- `hft.stage` — pipeline stage name (`validate`, `risk-check`, `match-call`, `pnl-calc`, …)
- `deployment.environment` — dev / staging / prod-sim

**Never attach raw JWTs, passwords, or PII to spans.** The shared `Tracing.USER_ID` key carries the
already-validated user UUID extracted from the JWT — never the token itself.

## Kafka trace propagation

`shared-kafka-lib` injects W3C `traceparent` / `tracestate` headers on every publish and extracts +
opens a CONSUMER span on every receive, so a trace started in HTTP flows through Kafka into
downstream services automatically.

## Custom metrics catalog

| Metric | Tags | Source |
|---|---|---|
| `order_submissions_total` | symbol, type, status | order-service |
| `order_processing_latency_seconds` | stage | order-service |
| `kafka_consumer_lag` | topic, consumer_group | kafka-clients |
| `risk_rejections_total` | reason | risk-engine |
| `risk_checks_duration_seconds` | — | risk-engine |
| `websocket_connections_active` | — | market-data-service |
| `orderbook_depth_gauge` | symbol, side | matching-engine (Rust) |
| `match_latency_microseconds` | — | matching-engine (Rust) |
| `orders_matched_total` | — | matching-engine (Rust) |
| `trades_executed_total` | — | matching-engine (Rust) |
| `engine_orders_processed_total` | symbol, type | matching-engine (Rust) |
| `engine_trades_total` | symbol | matching-engine (Rust) |
| `engine_snapshots_total` | symbol | matching-engine (Rust) |
| `engine_rejects_total` | reason | matching-engine (Rust) |
| `kafka_dlq_received_total` | topic, origin, errorClass | shared-kafka-lib (DLQMonitor) |

## Alerts

Configured in `infra/prometheus/alerts.yml`:

- `KafkaConsumerLagHigh` — lag > 10k for 5 minutes
- `MatchingEngineDown` — `up{service="matching-engine"} == 0` for 30s
- `RiskRejectionSpike` — rejections > 100/min
- `OrderProcessingLatencyHigh` — p99 > 500ms

Wire `prometheus` to Alertmanager when needed; this stack ships rules only.
