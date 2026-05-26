// k6 WebSocket stress: 1,000 concurrent connections to /ws/orderbook/BTC-USDT.
// Each socket holds 60s; success = at least one update received before timeout.
//
// Run:
//   WS_URL=ws://localhost:8083/ws/orderbook/BTC-USDT k6 run scripts/load-test/websocket-stress.js

import ws from 'k6/ws';
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const URL = __ENV.WS_URL || 'ws://localhost:8083/ws/orderbook/BTC-USDT';

const opened       = new Counter('ws_connections_opened');
const messages     = new Counter('ws_messages_received');
const handshakeErr = new Rate('ws_handshake_errors');

export const options = {
    scenarios: {
        wsClients: {
            executor: 'ramping-vus',
            startVUs: 0,
            stages: [
                { duration: '20s', target: 1000 },
                { duration: '40s', target: 1000 },
                { duration: '5s',  target: 0    },
            ],
        },
    },
    thresholds: {
        ws_handshake_errors: ['rate<0.02'],
        ws_messages_received: ['count>500'],
    },
};

export default function () {
    const res = ws.connect(URL, {}, function (socket) {
        opened.add(1);
        let got = false;
        socket.on('message', () => { got = true; messages.add(1); });
        socket.setTimeout(() => socket.close(), 60000);
        socket.on('close', () => check(null, { 'received any message': () => got }));
    });
    handshakeErr.add(!(res && res.status === 101));
}
