# shared-kafka-lib

Shared, reactive Kafka primitives for HFT services. Built on **reactor-kafka** (non-blocking).
Spring Boot auto-configuration wires everything when this jar is on the classpath.

## Modules

| Class | Purpose |
|---|---|
| `SharedKafkaProperties` | `hft.kafka.*` config binding (bootstrap servers, producer/consumer/dlq/admin) |
| `KafkaSenderFactory` / `KafkaReceiverFactory` | Build `SenderOptions` / `ReceiverOptions` from config |
| `KafkaEventPublisher<T extends EventEnvelope>` | Async publish; stamps headers; returns `Mono<RecordMetadata>` |
| `KafkaEventConsumer<T>` | Reactive at-least-once consumer; retry → DLQ; manual offset commit |
| `DLQRouter` | Wraps failed records in `DLQMessage` and publishes to `dlq.{topic}` |
| `DLQMonitor` | Subscribes to `dlq.*` pattern; logs + Micrometer counters |
| `DLQReplayService` + `DLQAdminController` | Operator-initiated replay from DLQ → original topic |
| `KafkaTopicAdmin` | AdminClient wrapper: create-if-absent + reactive health check |

## Event records

- `OrderEvent` (NEW / CANCEL / MODIFY)
- `TradeEvent`
- `MarketDataEvent` (TICK / TRADE_PRINT / BBO)
- `PortfolioEvent` (POSITION_UPDATE / PNL_SNAPSHOT / MARGIN_CALL)
- `RiskEvent` (LIMIT_BREACH / VAR_ALERT / KILL_SWITCH / RECOVERY)
- `OrderbookUpdateEvent` (SNAPSHOT / DELTA)
- `DLQMessage` (wrapper)

All records implement `EventEnvelope` so the publisher can extract a partition key.

## Required headers

Every record carries:

| Header | Source |
|---|---|
| `x-trace-id` | caller-provided or generated |
| `x-span-id` | generated per publish |
| `x-timestamp` | publish-time ISO-8601 |
| `x-service-origin` | `hft.kafka.service-origin` property |
| `x-event-type` | `event.getClass().getSimpleName()` |
| `x-retry-count`, `x-original-topic` | stamped by DLQ router on failed records |

## Consumer group naming

`{service-origin}-{topic}-group` — derived automatically by `KafkaReceiverFactory.defaultGroup(topic)`.
Service must set `hft.kafka.service-origin`.

## At-least-once vs exactly-once

### At-least-once (default)

```yaml
hft.kafka:
  producer:
    acks: all
    idempotence: true
    retries: 2147483647     # bounded by delivery.timeout.ms
  consumer:
    auto-commit: false
    read-committed: false
    max-retries: 3
    retry-backoff-ms: 200
```

- **Producer:** idempotent + `acks=all` prevents duplicates within a single producer session and tolerates broker failover without data loss.
- **Consumer:** manual offset commit only after handler success; retries on failure; DLQ on exhaustion.
- **Trade-off:** Duplicates possible if the consumer crashes after side-effect but before commit. Handlers must therefore be **idempotent** (use upserts / dedup tables / idempotency keys).
- **Throughput:** highest among the three modes.

### Exactly-once (EOS) — opt-in per producer/consumer

```yaml
hft.kafka:
  producer:
    acks: all
    idempotence: true
    transactional-id: order-service-1     # MUST be stable per producer instance
  consumer:
    read-committed: true                  # ignores uncommitted records
```

Pattern (consume-process-produce):

```java
kafkaSender.transactionManager().begin()
  .thenMany(processBatch())
  .then(kafkaSender.transactionManager().commit())
  .onErrorResume(e -> kafkaSender.transactionManager().abort());
```

- **Producer:** writes are buffered until `commit()`. Aborted transactions roll back automatically.
- **Consumer:** `isolation.level=read_committed` ensures it never sees rolled-back records.
- **Trade-offs:**
  - Latency: extra round trip per transaction; typically 2–5× p99 vs at-least-once.
  - Throughput: lower (cannot reorder records across partitions inside a transaction).
  - Operational: `transactional.id` must be **stable** across restarts to recover open transactions; usually mapped to a partition or pod identity.
  - Cross-system: transactions only span Kafka. If the side effect is an external DB write, you must still implement idempotency.

### Rule of thumb

| Situation | Mode |
|---|---|
| Idempotent handler with upserts (most services) | **at-least-once** |
| Pure Kafka-to-Kafka pipeline (e.g. stream processor) | **exactly-once** |
| Side effects on downstream DB without natural idempotency | **at-least-once + idempotency key in payload** |
| Order submission → matching engine | at-least-once; matching engine deduplicates via `idempotencyKey` |

## Quick start

```yaml
hft.kafka:
  bootstrap-servers: kafka:9092
  service-origin: order-service
  dlq:
    monitor-enabled: true
    admin-endpoint-enabled: false   # ops-only services
```

Producer use:

```java
@Autowired KafkaEventPublisher<EventEnvelope> publisher;

publisher.publish("orders", orderEvent)
        .doOnSuccess(meta -> log.info("published p={} o={}", meta.partition(), meta.offset()))
        .subscribe();
```

Consumer use:

```java
@Autowired KafkaReceiverFactory rfactory;
@Autowired ObjectMapper kafkaObjectMapper;
@Autowired SharedKafkaProperties props;
@Autowired DLQRouter dlqRouter;

KafkaEventConsumer<OrderEvent> consumer = new KafkaEventConsumer<>(
        rfactory.options("orders"),
        kafkaObjectMapper, OrderEvent.class, props, dlqRouter);

consumer.start(msg -> handleOrder(msg.payload()));
```

Topic admin (testing or bootstrap):

```java
@Autowired KafkaTopicAdmin admin;
admin.createTopicIfAbsent("orders", 6, (short) 1, Map.of("cleanup.policy", "delete"))
     .subscribe();
admin.healthCheck().subscribe(r -> log.info("kafka healthy={} latency={}ms", r.healthy(), r.latencyMs()));
```

DLQ replay (operator):

```
POST /admin/dlq/replay?topic=dlq.orders&maxMessages=50&timeoutSec=30
```

## Notes

- Producer-level `retries` is set to `Integer.MAX_VALUE` and bounded by `delivery.timeout.ms` (idempotent + retries is safe).
- `KafkaEventPublisher` adds **one** extra reactive retry envelope above the producer retries — this is the failure boundary that surfaces non-retryable errors (`AuthenticationException`, `AuthorizationException`, `SerializationException`).
- `KafkaEventConsumer` applies `maxRetries` per record (default 3) before DLQ routing.
- DLQ topics are auto-named `dlq.{originalTopic}`. They must be pre-created or created via `KafkaTopicAdmin.createTopicIfAbsent`.
