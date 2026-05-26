# matching-engine

Rust matching engine for the HFT platform. Cargo workspace.

## Crates

| Crate | Kind | Purpose |
|---|---|---|
| `orderbook`     | lib   | Limit order book + matching logic. No I/O, no async. |
| `kafka-bridge`  | lib   | rdkafka async consumer + transactional producer. |
| `redis-bridge`  | lib   | Async Redis snapshot persistence. |
| `proto-gen`     | lib   | prost build script (order/trade/orderbook .proto). |
| `matching-engine` | bin | Orchestration loop. |

## Layout

```
matching-engine/
├── Cargo.toml                 # workspace
├── Dockerfile
├── proto/{order,trade,orderbook}.proto
└── crates/
    ├── orderbook/             # core matching
    ├── kafka-bridge/          # rdkafka I/O
    ├── redis-bridge/          # redis-rs snapshot I/O
    ├── proto-gen/             # prost-build
    └── matching-engine/       # binary
```

## Build

```bash
# native build (needs cmake + libsasl2-dev + libssl-dev + protobuf-compiler)
cargo build --release --bin matching-engine

# run tests
cargo test -p orderbook

# run benches
cargo bench -p orderbook

# docker
docker build -t hft/matching-engine -f Dockerfile .
```

## Configuration

Loaded via `config` crate. Sources (priority high→low):
1. Env vars: `MATCHING__KAFKA__BOOTSTRAP_SERVERS`, etc. (`__` = nesting).
2. File `matching-engine.{toml,yaml}` next to binary.
3. Defaults baked in `config_loader.rs`.

Key knobs:

| Key | Default |
|---|---|
| `symbols` | `[BTC-USDT, ETH-USDT, SOL-USDT]` |
| `kafka.bootstrap_servers` | `localhost:29092` |
| `kafka.consumer_group` | `matching-engine-orders-group` |
| `kafka.transactional_id` | `matching-engine-tx-1` |
| `kafka.read_committed` | `true` |
| `redis.url` | `redis://localhost:6379` |
| `redis.ttl_secs` | `60` |
| `snapshot.interval_ms` | `100` |
| `snapshot.event_threshold` | `1000` |

## Performance

Targets:
- Single order match: **< 100µs** p99 — verify via `cargo bench`.
- Throughput: **> 100K orders/sec** single-core (no I/O — see benches/match_bench.rs).
- Hot path uses pre-allocated `VecDeque` (capacity 16) per level; `Vec::with_capacity(4)` for trade buffers.

Notes:
- `BTreeMap` instead of `BinaryHeap` because we need both `peek_best` (O(log n)) and `cancel_order_by_id` (O(log n)) without rebuild.
- `rust_decimal::Decimal` everywhere — never `f64`.
- Hot path is single-threaded per symbol (no locks inside `OrderBook`); the engine wraps it in a `tokio::Mutex` so the async consumer can drive it.

## Exactly-once

- Producer: `enable.idempotence=true`, `transactional.id=<stable>`, `acks=all`.
- Consumer: `isolation.level=read_committed`, manual `commit` only after successful tx commit.
- Trade publish and snapshot publish are wrapped in a single Kafka transaction in `TradePublisher::publish_batch`.
- On publish failure: transaction aborted, consumer does NOT commit → broker redelivers → engine re-processes (state is rebuilt from snapshot + replay).

## Snapshot cadence

Either condition fires a Redis snapshot write:
1. `events_since_snapshot ≥ snapshot.event_threshold` (default 1000)
2. `last_snapshot_at.elapsed() ≥ snapshot.interval_ms` (default 100ms)

Per-symbol top-20 levels (`SNAPSHOT_DEPTH`). TTL refreshed via `SET EX 60` on every write.

## On startup

For each configured symbol:
1. `SnapshotClient.read("orderbook:{symbol}")` — if present, restore via `OrderBook::restore_from_snapshot`.
2. Kafka consumer subscribes from last committed offset (or `auto.offset.reset=earliest` if no commit).
3. Orders since the snapshot's sequence number replay and rebuild full per-order state.

## Constraints honoured

- `#![deny(unsafe_code)]` on every crate.
- All errors propagated via `Result<_, BookError | BridgeError | RedisError>`. No `unwrap` in production paths (test code uses `unwrap` only).
- Engine binary runs in its own Docker container (see Dockerfile).
- All price arithmetic uses `rust_decimal::Decimal` (string serde for wire compatibility with the Java services).
