#!/usr/bin/env bash
# Chaos: introduce network delay between order-service and Kafka using tc/netem
# inside the order-service container. Requires NET_ADMIN cap on the container:
#
#   docker run --cap-add NET_ADMIN ...
#
# Args:
#   DURATION_SECONDS  — how long to keep the delay active (default 30)
#   DELAY_MS          — RTT delay to inject (default 300ms)
#   TARGET_CONTAINER  — container to apply tc to (default order-service)
#
# Cleanup is automatic in the trap.
set -euo pipefail

TARGET=${TARGET_CONTAINER:-order-service}
DELAY=${DELAY_MS:-300}
DURATION=${DURATION_SECONDS:-30}
IFACE=${IFACE:-eth0}

cleanup() {
    echo "[chaos] removing network delay on $TARGET..."
    docker exec "$TARGET" tc qdisc del dev "$IFACE" root 2>/dev/null || true
}
trap cleanup EXIT

echo "[chaos] applying ${DELAY}ms delay on $TARGET:$IFACE for ${DURATION}s..."
docker exec "$TARGET" tc qdisc add dev "$IFACE" root netem delay "${DELAY}ms"
sleep "$DURATION"
echo "[chaos] window elapsed; cleanup will remove qdisc"
