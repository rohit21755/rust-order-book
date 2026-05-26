# HFT Simulation Platform

Production-grade simulation of a high-frequency trading platform — Java microservices
on Spring Boot + WebFlux, a Rust matching engine, Kafka event backbone, full
event-sourcing + CQRS, and a complete observability stack.

> Built for FAANG / HFT interview prep. Every component is exercised by automated
> tests, load tests, and chaos scripts.

---

## 1. Architecture

```
┌────────────────────────── Clients (REST / WebSocket / FIX 4.2) ──────────────────────────┐
└───────────────────────────────────────┬───────────────────────────────────────────────────┘
                                        ▼
                              ┌─────────────────────┐
                              │     API Gateway     │   (Spring Cloud Gateway, JWT validate)
                              └─────────┬───────────┘
       ┌──────────────┬─────────────────┼──────────────────────┬──────────────┐
       ▼              ▼                 ▼                      ▼              ▼
  ┌─────────┐   ┌──────────┐    ┌────────────────┐     ┌──────────────┐  ┌─────────┐
  │  Auth   │   │  Order   │    │  Market Data   │     │  Portfolio   │  │  Risk   │
  │ Service │   │ Service  │    │    Service     │     │   Service    │  │ Engine  │
  └────┬────┘   └────┬─────┘    └───────┬────────┘     └──────┬───────┘  └────┬────┘
       │             │   ▲              │                     │               │
       │     gRPC ◀──┘   │              │                     │               │
       │             ▼   │              │                     │               │
       │      ┌────────────────┐        │                     │               │
       │      │  Rust Matching │◀───────┴─────────────────────┴───────────────┘
       │      │     Engine     │
       │      └────────┬───────┘
       │               │
       ▼               ▼
┌──────────────────────────────────── Apache Kafka ─────────────────────────────────────────┐
│   orders   trades   orderbook-updates   portfolio-events   risk-events   dlq.*           │
└──────────────────────────────────────────────────────────────────────────────────────────┘
        │                          │                       │                  │
        ▼                          ▼                       ▼                  ▼
  PostgreSQL                   ClickHouse                Redis            Prometheus
  (orders + events             (OHLCV candles)         (cache,           + Grafana
  + portfolio + auth)                                  snapshots)        + Jaeger
```

See `HFT_System_Context.md` for the canonical knowledge graph + per-service contracts.

---

## 2. Components

| Service | Port | Stack | Highlights |
|---|---:|---|---|
| `auth-service`         | 8081 | Spring WebFlux, R2DBC, Redis | JWT (HS256), refresh tokens, RBAC, API keys (SHA-256 stored), Redis sliding-window rate limit |
| `order-service`        | 8082 + FIX 9876 | Spring WebFlux, R2DBC, Reactor-Kafka | Event sourcing + CQRS, snapshot/replay, gRPC → engine, **FIX 4.2 acceptor** |
| `market-data-service`  | 8083 | WebFlux + Netty | WebSocket fan-out via `Sinks.Many`, backpressure (drop-oldest 1k buffer), Redis cache, ClickHouse OHLCV |
| `portfolio-service`    | 8084 | WebFlux, R2DBC | Trade settlement (exactly-once via `processed_trades`), `@Version` optimistic locking, Decimal-only PnL |
| `risk-engine`          | 8085 | WebFlux | 5 pre-trade checks, hot-reloadable params via `AtomicReference`, halt registry, all Redis-cached so eval < 10ms |
| `api-gateway`          | 8080 | Spring Cloud Gateway | Routing, rate-limit, JWT validate |
| `matching-engine` (Rust) | gRPC 50051 / metrics 9091 | tonic + reactor-kafka (Rust) | `BTreeMap`-based bid/ask, price-time FIFO, stop-loss, snapshot every 100ms / 1k events, **no `unsafe`** |
| `shared-lib`             | — | jar | Reactive JWT validator, `BusinessException`, error codes |
| `shared-kafka-lib`       | — | jar (Spring autoconfig) | Reactive publish/consume, idempotent producer, DLQ router + monitor + replay, **W3C trace propagation** |
| `shared-grpc-lib`        | — | jar | gRPC pool, Resilience4j circuit breaker, Redis fallback for orderbook |
| `shared-observability-lib` | — | jar | OTel SDK + Micrometer + manual-span helper `Tracing` |

---

## 3. Run locally (Docker Compose)

```bash
# Build images (each service has its own Dockerfile)
for s in services/{auth,order,market-data,portfolio,risk-engine,api-gateway}-service*; do
  docker build -t hft/$(basename $s):0.1.0 -f $s/Dockerfile .
done
docker build -t hft/matching-engine:0.1.0 -f matching-engine/Dockerfile matching-engine

# Bring up infra + services
docker compose up -d zookeeper kafka redis postgres clickhouse
docker compose up -d auth-service order-service market-data-service portfolio-service risk-engine api-gateway matching-engine

# Observability overlay
docker network create hft-network 2>/dev/null || true
docker compose -f docker-compose.yml -f infra/docker/observability.compose.yml up -d jaeger prometheus grafana
```

| URL | Purpose |
|---|---|
| http://localhost:8080 | API gateway |
| ws://localhost:8083/ws/orderbook/BTC-USDT | Market data WS |
| http://localhost:16686 | Jaeger |
| http://localhost:9090  | Prometheus |
| http://localhost:3000  | Grafana (admin/admin) |

---

## 4. Deploy to Kubernetes (minikube / kind / EKS / GKE)

```bash
# Local: minikube
minikube start --cpus 6 --memory 12g
eval $(minikube docker-env)
# build images into the minikube docker daemon (same loop as above)

# Apply manifests
kubectl apply -f infra/kubernetes/cluster/namespace.yaml
kubectl apply -f infra/kubernetes/cluster/                # postgres, redis, kafka, ingress, networkpolicy
kubectl apply -f infra/kubernetes/services/               # all 7 services

# Wait until rollout completes
kubectl -n hft-platform rollout status deployment/auth-service
kubectl -n hft-platform get hpa
```

Manifests work unmodified on EKS / GKE — `LoadBalancer` service type and standard
`PersistentVolumeClaim` resolve via the cluster's default storage class + cloud LB.

NetworkPolicies enforce: each service only egresses to its declared dependencies.

---

## 5. Performance benchmarks

Single-node dev cluster (16-core, 32 GB):

| Surface | Target | Achieved |
|---|---:|---:|
| Matching engine single-match p99 | < 100 µs | ~45 µs (criterion bench) |
| Matching engine throughput, 1 core | > 100k orders/sec | ~150k inserts/sec |
| Order service submit p99 (end-to-end) | < 250 ms | ~180 ms under 10k rps |
| Risk eval p99 (warm cache) | < 10 ms | ~6 ms |
| WebSocket fan-out | 1k concurrent clients | 1k sustained, < 2% handshake errors |

Reproduce: `scripts/load-test/` (k6); see that folder's `README.md`.

---

## 6. Design decisions + trade-offs

| Decision | Why | Trade-off |
|---|---|---|
| **Rust for matching engine** | Need µs-grade match latency, zero-GC | More languages in stack; Rust→Java JSON bridge |
| **`BTreeMap` (not heap) for book** | O(log n) cancel-by-id; iter best | Slightly higher constant factor than `BinaryHeap` |
| **Event sourcing + CQRS in order-service** | Audit trail + replay + projection rebuild | Doubles persistence surface; eventual consistency on reads |
| **Kafka exactly-once (transactional producer + read_committed)** | No duplicate trades downstream | 2–5× latency vs at-least-once; transactional.id must be stable |
| **Reactor-Kafka, not spring-kafka** | Non-blocking on WebFlux event loop | Sharper learning curve; fewer Spring conveniences |
| **R2DBC, not JDBC** | Backpressure all the way to DB | Limited driver maturity; single-connection per txn |
| **Decimal everywhere** | Money math without float drift | More allocations; verbose API |
| **Optimistic locking (`@Version`)** | High write concurrency, no pessimistic deadlocks | Conflict-retry logic in caller |
| **Redis sliding window for activity** | Sub-ms anomaly detect | Memory grows with active users |
| **Self-trade prevention re-inserts maker at FRONT** | Preserve time priority post-reject | More code paths through cross loop |

---

## 7. FAANG interview talking points per component

### Order Service
- **Event sourcing**: monotonic per-aggregate sequence enforced by `UNIQUE(aggregate_id, sequence_number)`, append-only via UPDATE/DELETE triggers raising, snapshot every 50 events, replay endpoint idempotent via processed_events ledger.
- **CQRS**: write side appends events + Kafka publish post-commit; read side updates denormalized table from projection, `last_sequence < EXCLUDED.last_sequence` guards against out-of-order delivery.
- **FIX 4.2 acceptor**: QuickFIX/J, maps `D` → `SubmitOrderCommand`, `F` → `CancelOrderCommand`, emits `8` ExecutionReport, audit log append-only.

### Matching Engine (Rust)
- `BTreeMap<Reverse<Decimal>, PriceLevel>` for bids vs `BTreeMap<Decimal, PriceLevel>` for asks gives O(log n) peek + O(log n) cancel-by-id via `order_index` hashmap.
- `rust_decimal::Decimal` — never `f64`; `#![deny(unsafe_code)]` everywhere.
- Snapshot-then-event-replay restore on restart: top-N levels from Redis + replay from last committed Kafka offset.
- Exactly-once: transactional producer + `read_committed` consumer; trades + snapshot publish in one Kafka tx.
- `match_latency_microseconds` histogram with 10/50/100/500/1000 µs buckets exposed via `/metrics`.

### Risk Engine
- 5 checks (leverage, exposure, max-order-value, daily-loss, price-deviation) + activity anomaly + halt; parallelized via `Mono.zip`, sub-10ms with warm Redis caches.
- Hot-reloadable parameters: `AtomicReference<RiskParameters>` swap on PUT — readers see consistent snapshot per evaluation, no restart.
- Halt registry in Redis set; short-circuit before any check.
- Approve/reject published to `risk-events` topic for audit + alerting.

### Market Data
- `Sinks.Many.multicast().onBackpressureBuffer(1000)` with drop-oldest → never blocks Kafka consumer thread.
- WebSocket heartbeat 30s + 2-missed-pong eviction.
- Subscription registry `ConcurrentHashMap<sessionId, Set<topic>>`.
- ClickHouse OHLCV batches (≥100 records OR 1s); REST hits Redis cache first.

### Portfolio
- Settlement runs in one R2DBC transaction (single connection → sequential `flatMap`, not `zipWith`).
- `processed_trades` PK = `tradeId` → duplicate trade deliveries silently skipped.
- Holdings quantity never negative via DB `CHECK` + service-side `min(holding, tradeQty)`.
- Weighted-average buy price using `BigDecimal` with `MathContext` HALF_UP.

### Auth
- JWT HS256 access (15 min) + opaque refresh in Redis (7 d).
- BCrypt strength 12 with `Mono.fromCallable + Schedulers.boundedElastic` so the netty event loop never blocks.
- API key shown once, SHA-256 stored.
- Redis sliding-window rate limit (ZSET) per user.

### Observability
- W3C `traceparent`/`tracestate` injected into every Kafka record header → trace flows engine ↔ order ↔ portfolio.
- Manual spans never carry PII; standard attribute keys = UUID + symbol + stage + environment.
- Grafana auto-provisions 5 dashboards from `infra/grafana/`.
- 4 alert rules (`KafkaConsumerLagHigh`, `MatchingEngineDown`, `RiskRejectionSpike`, `OrderProcessingLatencyHigh`).

### Resilience
- Resilience4j circuit breaker on order → engine gRPC; Redis snapshot fallback.
- Risk-engine-down: configurable halt (default) vs bypass.
- Matching-engine-down: orders accumulate in Kafka, replay on recovery from snapshot.
- DLQ topics (`dlq.*`) + replay endpoint.

---

## 8. Chaos + load + audit

| Folder | What |
|---|---|
| `scripts/chaos/` | Kill containers (engine, kafka), network-partition (`tc/netem`), consistency-verify |
| `scripts/load-test/` | k6: order-flood (10k rps), websocket-stress (1k clients), mixed-load (70/30) |
| `scripts/fix-test-client/` | Python QuickFIX initiator |
| `infra/OBSERVABILITY.md` | Per-service OTel + Prometheus integration |
| `infra/GRACEFUL_DEGRADATION.md` | Service-down behavior matrix |

---

## 9. Build order (Maven)

```
shared-lib                → mvn -pl services/shared-lib install
shared-kafka-lib          → mvn -pl services/shared-kafka-lib install
shared-grpc-lib           → mvn -pl services/shared-grpc-lib install
shared-observability-lib  → mvn -pl services/shared-observability-lib install
# then any service
mvn -pl services/order-service package
```

Rust: `cargo build --release -p matching-engine` (needs `protoc`, `cmake`, `libsasl2-dev`, `libssl-dev`).

---

## 10. Repo layout

```
.
├── HFT_System_Context.md         # canonical knowledge graph
├── docker-compose.yml            # local infra + services
├── matching-engine/              # Rust workspace
│   ├── crates/{matching-engine,orderbook,kafka-bridge,redis-bridge,grpc-server,proto-gen}/
│   └── proto/
├── services/                     # Java services + shared libs
│   ├── auth-service/
│   ├── order-service/
│   ├── market-data-service/
│   ├── portfolio-service/
│   ├── risk-engine/
│   ├── api-gateway/
│   ├── shared-lib/
│   ├── shared-kafka-lib/
│   ├── shared-grpc-lib/
│   └── shared-observability-lib/
├── proto/                        # gRPC + cross-language event protos
├── infra/
│   ├── docker/                   # compose overlays (observability)
│   ├── kubernetes/{cluster,services}/
│   ├── prometheus/
│   ├── grafana/
│   ├── OBSERVABILITY.md
│   └── GRACEFUL_DEGRADATION.md
└── scripts/
    ├── chaos/
    ├── load-test/
    └── fix-test-client/
```

License: MIT.
