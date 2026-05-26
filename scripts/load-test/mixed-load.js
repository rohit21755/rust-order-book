// k6 mixed load: 70% reads / 30% writes. Realistic browse-then-trade profile.
//
// Reads: /api/orders/book/{symbol}, /api/market/ticker/{symbol}, /api/portfolio
// Writes: /api/orders (submit) + occasional cancel

import http from 'k6/http';
import { check, group } from 'k6';
import { Trend, Rate } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const JWT  = __ENV.JWT || '';
const SEED = 7;

const SYMBOLS = ['BTC-USDT', 'ETH-USDT', 'SOL-USDT'];
let rng = SEED;
function rnd() { rng = (rng * 1664525 + 1013904223) >>> 0; return rng / 0xFFFFFFFF; }

const submitLat = new Trend('mix_submit_latency_ms');
const readLat   = new Trend('mix_read_latency_ms');
const errors    = new Rate('mix_errors');

export const options = {
    scenarios: {
        mixed: {
            executor: 'ramping-arrival-rate',
            startRate: 100,
            timeUnit: '1s',
            preAllocatedVUs: 100,
            maxVUs: 500,
            stages: [
                { duration: '30s', target: 1000 },
                { duration: '2m',  target: 1000 },
                { duration: '10s', target: 0    },
            ],
        },
    },
    thresholds: {
        mix_errors: ['rate<0.02'],
        mix_submit_latency_ms: ['p(99)<300'],
        mix_read_latency_ms:   ['p(99)<150'],
    },
};

function authHeaders() {
    return JWT ? { 'Content-Type': 'application/json', 'Authorization': `Bearer ${JWT}` }
               : { 'Content-Type': 'application/json' };
}

export default function () {
    const r = rnd();
    const sym = SYMBOLS[Math.floor(rnd() * SYMBOLS.length)];
    if (r < 0.7) {
        // READ path
        group('reads', () => {
            const t0 = Date.now();
            const which = Math.floor(rnd() * 3);
            let res;
            if (which === 0) res = http.get(`${BASE}/api/orders/book/${sym}?depth=10`, { headers: authHeaders() });
            else if (which === 1) res = http.get(`${BASE}/api/market/ticker/${sym}`,    { headers: authHeaders() });
            else res = http.get(`${BASE}/api/portfolio`,                                 { headers: authHeaders() });
            readLat.add(Date.now() - t0);
            errors.add(!check(res, { '2xx': r => r.status >= 200 && r.status < 300 }));
        });
    } else {
        // WRITE path
        group('submit', () => {
            const body = {
                symbol: sym, side: rnd() > 0.5 ? 'BUY' : 'SELL', type: 'LIMIT',
                price: (100 + Math.floor(rnd() * 50)).toFixed(2),
                quantity: (0.01 + rnd() * 0.2).toFixed(8),
                idempotencyKey: `mix-${__VU}-${__ITER}-${SEED}`,
            };
            const t0 = Date.now();
            const res = http.post(`${BASE}/api/orders`, JSON.stringify(body), { headers: authHeaders() });
            submitLat.add(Date.now() - t0);
            errors.add(!check(res, { '201/200': r => r.status === 201 || r.status === 200 }));
        });
    }
}
