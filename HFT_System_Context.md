# HFT Platform — System Knowledge Graph & Architecture Context
> **Attach this file to EVERY phase prompt** when working with Claude Code or Google Antigravity. It is the shared memory of the entire system.

---

## 1. PROJECT IDENTITY

**Name:** HFT Simulation Platform  
**Inspired by:** Binance, Robinhood, Coinbase, NYSE, Nasdaq  
**Purpose:** Demonstrate production-grade HFT infrastructure for FAANG/HFT interviews  
**Core Properties:** Low-latency, distributed, event-driven, reactive, observable

---

## 2. SERVICE KNOWLEDGE GRAPH

```
┌─────────────────────────────────────────────────────────────────┐
│                        CLIENT LAYER                             │
│              WebSocket / REST / FIX Protocol                    │
└───────────────────────┬─────────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────────┐
│                    API GATEWAY                                   │
│         (Spring Cloud Gateway — routing, rate limiting)         │
└──┬────────┬──────────┬──────────────┬──────────────┬────────────┘
   │        │          │              │              │
   ▼        ▼          ▼              ▼              ▼
┌──────┐ ┌──────┐ ┌─────────┐ ┌──────────┐ ┌────────────┐
│ AUTH │ │ORDER │ │ MARKET  │ │PORTFOLIO │ │   RISK     │
│ SVC  │ │ SVC  │ │  DATA   │ │  SVC     │ │  ENGINE    │
│      │ │      │ │  SVC    │ │          │ │            │
└──┬───┘ └──┬───┘ └────┬────┘ └────┬─────┘ └─────┬──────┘
   │        │          │           │              │
   │        ▼          │           │              │
   │  ┌───────────┐    │           │              │
   │  │   RUST    │    │           │              │
   │  │ MATCHING  │    │           │              │
   │  │  ENGINE   │    │           │              │
   │  └─────┬─────┘    │           │              │
   │        │          │           │              │
   └────────┴──────────┴───────────┴──────────────┘
                        │
            ┌───────────▼───────────┐
            │     APACHE KAFKA      │
            │  (Event Backbone)     │
            │  Topics:              │
            │  • orders             │
            │  • trades             │
            │  • market-data        │
            │  • portfolio-events   │
            │  • risk-events        │
            │  • orderbook-updates  │
            │  • dlq.*              │
            └───────────┬───────────┘
                        │
     ┌──────────────────┼──────────────────┐
     │                  │                  │
     ▼                  ▼                  ▼
┌─────────┐      ┌────────────┐    ┌──────────────┐
│ REDIS   │      │ POSTGRESQL │    │  CLICKHOUSE  │
│ • order │      │ • users    │    │  (time-series│
│   book  │      │ • orders   │    │   candle data│
│ • sess. │      │ • portfolio│    │   tick data) │
│ • snap. │      │ • trades   │    └──────────────┘
└─────────┘      └────────────┘
```

---

## 3. SERVICE DEPENDENCY MAP

| Service | Depends On | Publishes To | Consumes From |
|---------|-----------|--------------|---------------|
| Auth Service | Redis, PostgreSQL | — | — |
| Order Service | Auth, Redis, PostgreSQL | orders, risk-events | — |
| Rust Matching Engine | Redis (snapshots) | trades, orderbook-updates | orders |
| Market Data Service | ClickHouse, Redis | market-data | trades, orderbook-updates |
| Portfolio Service | PostgreSQL, Redis | portfolio-events | trades |
| Risk Engine | Redis, PostgreSQL | risk-events | orders, portfolio-events |
| API Gateway | All services | — | — |

---

## 4. KAFKA TOPIC SCHEMA

```
TOPIC: orders
  Key: orderId
  Value: { orderId, userId, symbol, side(BUY/SELL), type(LIMIT/MARKET/STOP), 
           price, quantity, timestamp, idempotencyKey }

TOPIC: trades
  Key: tradeId
  Value: { tradeId, buyOrderId, sellOrderId, symbol, price, quantity, 
           executedAt, makerUserId, takerUserId }

TOPIC: market-data
  Key: symbol
  Value: { symbol, lastPrice, bidPrice, askPrice, volume24h, 
           priceChange24h, timestamp }

TOPIC: portfolio-events
  Key: userId
  Value: { userId, eventType(TRADE_SETTLED/BALANCE_UPDATED), 
           symbol, quantity, price, pnl, timestamp }

TOPIC: risk-events
  Key: orderId
  Value: { orderId, userId, riskType(LEVERAGE/EXPOSURE/ANOMALY), 
           status(APPROVED/REJECTED), reason, timestamp }

TOPIC: orderbook-updates
  Key: symbol
  Value: { symbol, bids[{price, qty}], asks[{price, qty}], 
           sequence, timestamp }

TOPIC: dlq.orders / dlq.trades (Dead Letter Queues)
  Value: { originalTopic, originalPayload, errorMessage, retryCount }
```

---

## 5. DATA STORE OWNERSHIP

| Store | Owner | Data |
|-------|-------|------|
| PostgreSQL | Auth, Order, Portfolio | users, orders, trades, holdings, PnL records |
| Redis | Auth, Order, Market Data, Risk | sessions, active orderbook, market snapshots, rate limits |
| ClickHouse | Market Data | OHLCV candles, tick data, historical trades |

---

## 6. TECH STACK REFERENCE

| Layer | Technology | Version |
|-------|-----------|---------|
| Backend Services | Spring Boot | 3.x |
| Reactive | Spring WebFlux + Project Reactor | — |
| WebSocket | Netty + Reactive Streams | — |
| Event Streaming | Apache Kafka | 3.x |
| Matching Engine | Rust | 1.75+ |
| Rust-Java Bridge | gRPC (tonic in Rust, grpc-java in Spring) | — |
| Cache / Session | Redis | 7.x |
| Primary DB | PostgreSQL | 15.x |
| Time-series | ClickHouse | 23.x |
| Containerization | Docker + Docker Compose | — |
| Orchestration | Kubernetes | 1.28+ |
| Monitoring | Prometheus + Grafana | — |
| Tracing | OpenTelemetry + Jaeger | — |
| Auth | JWT + OAuth2 + RBAC | — |
| Build | Maven (Java) + Cargo (Rust) | — |

---

## 7. REPO STRUCTURE

```
hft-platform/
├── services/
│   ├── auth-service/          (Spring Boot)
│   ├── order-service/         (Spring Boot + WebFlux)
│   ├── market-data-service/   (Spring Boot + WebFlux)
│   ├── portfolio-service/     (Spring Boot + WebFlux)
│   ├── risk-engine/           (Spring Boot + WebFlux)
│   └── api-gateway/           (Spring Cloud Gateway)
├── matching-engine/           (Rust)
├── proto/                     (shared .proto files for gRPC)
├── infra/
│   ├── docker/
│   ├── kubernetes/
│   ├── prometheus/
│   └── grafana/
├── scripts/                   (start, seed, load-test scripts)
├── docs/
│   ├── architecture.md
│   └── api-reference.md
└── docker-compose.yml
```

---

## 8. EVENT FLOW: ORDER LIFECYCLE

```
User → POST /api/orders
  → Order Service (validate, assign ID, idempotency check)
    → Risk Engine (leverage + exposure check via risk-events)
      → [APPROVED] → Kafka: orders topic
        → Rust Matching Engine (consume orders)
          → [MATCHED] → Kafka: trades + orderbook-updates
            → Portfolio Service (consume trades → update holdings, PnL)
            → Market Data Service (consume trades → update ticker, candles)
        → [UNMATCHED] → stays in orderbook (Redis snapshot)
      → [REJECTED] → order status = REJECTED, notify via WebSocket
```

---

## 9. PHASE MAP (which AI builds what)

| Phase | Scope | Primary Tool | Secondary Tool |
|-------|-------|-------------|----------------|
| 1 | Repo + Docker infra | Claude Code | Antigravity (Docker configs) |
| 2 | Auth Service | Claude Code | Antigravity (boilerplate) |
| 3 | Order Service (REST + validation) | Claude Code | Antigravity (controllers) |
| 4 | Kafka Integration Layer | Claude Code | — |
| 5 | Rust Matching Engine (core) | Claude Code | — |
| 6 | Rust-Java gRPC Bridge | Claude Code | Antigravity (proto files) |
| 7 | Market Data Service + WebSocket | Claude Code | Antigravity (WebSocket configs) |
| 8 | Portfolio Service | Antigravity | Claude Code (Kafka consumers) |
| 9 | Risk Engine | Claude Code | Antigravity (REST endpoints) |
| 10 | Event Sourcing + CQRS + DLQ | Claude Code | — |
| 11 | Observability (OTel, Jaeger, Prometheus, Grafana) | Antigravity | Claude Code (instrumentation) |
| 12 | Kubernetes + Load Testing + FIX Protocol | Claude Code | Antigravity (K8s YAML) |

---

## 10. CRITICAL CONSTRAINTS (apply to all phases)

- Every service must be independently deployable via Docker
- All inter-service communication is async via Kafka (no direct service-to-service HTTP calls except Auth validation)
- Idempotency keys on all order submissions — exactly-once processing
- All Kafka consumers must handle DLQ routing on failure (max 3 retries)
- Redis keys must have TTL set — never unbounded
- All endpoints must be reactive (WebFlux, no blocking I/O)
- Every phase must include: working code + unit tests + Dockerfile + README section
- No hardcoded config — use application.yml + environment variables

