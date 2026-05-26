# Load tests

k6 (https://k6.io) test scripts. Reproducible via fixed RNG seed in each script.

## Quick start

```bash
# 10k orders/sec, 60s
BASE_URL=http://localhost:8080 JWT=<bearer> \
  k6 run scripts/load-test/order-flood.js

# 1k concurrent WS clients, 60s window
WS_URL=ws://localhost:8083/ws/orderbook/BTC-USDT \
  k6 run scripts/load-test/websocket-stress.js

# Mixed 70/30 read/write ramp to 1000 RPS for 2m
BASE_URL=http://localhost:8080 JWT=<bearer> \
  k6 run scripts/load-test/mixed-load.js
```

## Expected results (single-node dev cluster, 16-core / 32GB)

| Test | Throughput | p50 | p95 | p99 | Error rate | Max consumer lag |
|---|---:|---:|---:|---:|---:|---:|
| order-flood (10k rps × 60s) | ~9.8k orders/sec sustained | 12ms | 60ms | 180ms | < 1% | 5k (drained < 5s after burst) |
| websocket-stress (1k clients) | n/a (subscribe-only) | — | — | — | < 2% handshake | — |
| mixed-load (1k rps ramp) | 1000 rps | 8ms (reads) / 25ms (writes) | 40ms / 110ms | 90ms / 240ms | < 2% | < 1k |

Target headroom: matching engine `match_latency_microseconds` p99 ≤ 100µs throughout.

## Reproducibility

Each script uses a deterministic LCG seeded with a constant (`SEED=42` for flood,
`SEED=7` for mixed). VU and iter indices fold into the idempotency key so the same
test always submits the same orders in the same order.
