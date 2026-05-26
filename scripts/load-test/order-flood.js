// k6 load test: order flood — 10,000 orders/sec for 60s.
// Reproducible via fixed seed + deterministic order data.
//
// Run:
//   BASE_URL=http://localhost:8080 JWT=eyJhbGciOi... \
//   k6 run --vus 200 --duration 60s scripts/load-test/order-flood.js
//
// Output: throughput, p50/p95/p99 latency, error rate.

import http from 'k6/http';
import { check } from 'k6';
import { Trend, Rate, Counter } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const JWT  = __ENV.JWT || '';
const SEED = 42;

const submitLatency = new Trend('order_submit_latency_ms');
const errorRate     = new Rate('order_errors');
const submitted     = new Counter('order_submitted_total');

const SYMBOLS = ['BTC-USDT', 'ETH-USDT', 'SOL-USDT', 'ADA-USDT', 'DOGE-USDT', 'XRP-USDT'];
const SIDES   = ['BUY', 'SELL'];

// Deterministic LCG so different VUs/iters produce stable order params.
let lcgState = SEED;
function nextRand() {
    lcgState = (lcgState * 1664525 + 1013904223) >>> 0;
    return lcgState / 0xFFFFFFFF;
}

export const options = {
    scenarios: {
        flood: {
            executor: 'constant-arrival-rate',
            rate: 10000,
            timeUnit: '1s',
            duration: '60s',
            preAllocatedVUs: 200,
            maxVUs: 500,
        },
    },
    thresholds: {
        'order_submit_latency_ms': ['p(50)<25', 'p(95)<100', 'p(99)<250'],
        'order_errors':            ['rate<0.01'],
        'http_req_duration':       ['p(95)<200'],
    },
};

export default function () {
    const r = nextRand();
    const symbol = SYMBOLS[Math.floor(r * SYMBOLS.length)];
    const side = SIDES[Math.floor(nextRand() * 2)];
    const basePrice = 100 + Math.floor(nextRand() * 100);
    const qty = (0.001 + nextRand() * 0.5).toFixed(8);

    const body = {
        symbol,
        side,
        type: 'LIMIT',
        price: basePrice.toFixed(2),
        quantity: qty,
        idempotencyKey: `lt-${__VU}-${__ITER}-${SEED}`,
    };
    const headers = JWT ? { 'Content-Type': 'application/json', 'Authorization': `Bearer ${JWT}` }
                        : { 'Content-Type': 'application/json' };
    const t0 = Date.now();
    const res = http.post(`${BASE}/api/orders`, JSON.stringify(body), { headers });
    submitLatency.add(Date.now() - t0);
    submitted.add(1);

    const ok = check(res, {
        '201/200': r => r.status === 201 || r.status === 200,
    });
    errorRate.add(!ok);
}
