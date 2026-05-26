#!/usr/bin/env bash
# Chaos: kill one Kafka broker; verify no message loss after recovery.
#
# Single-broker dev stack: kill then restart, then count messages on `orders` topic
# before/after. With acks=all + idempotent producer, the count should be preserved
# once the broker is back.
set -euo pipefail

CONTAINER=${CONTAINER:-kafka}
ORDERS_TOPIC=${ORDERS_TOPIC:-orders}
DOWN_SECONDS=${DOWN_SECONDS:-15}

count_offsets() {
    docker exec "$CONTAINER" kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list localhost:9092 --topic "$ORDERS_TOPIC" --time -1 \
        | awk -F: '{sum += $3} END {print sum+0}'
}

echo "[chaos] sampling orders HWM before broker kill..."
BEFORE=$(count_offsets)
echo "  HWM before: $BEFORE"

echo "[chaos] killing $CONTAINER (SIGKILL)..."
docker kill "$CONTAINER"

echo "[chaos] sleeping ${DOWN_SECONDS}s with broker down..."
sleep "$DOWN_SECONDS"

echo "[chaos] starting $CONTAINER..."
docker start "$CONTAINER"

echo "[chaos] waiting 30s for broker re-elect + ISR catch-up..."
sleep 30

AFTER=$(count_offsets)
echo "  HWM after: $AFTER"

if (( AFTER >= BEFORE )); then
    echo "[chaos] PASS — no message loss (HWM non-decreasing)"
    exit 0
else
    echo "[chaos] FAIL — HWM regressed: lost $((BEFORE - AFTER)) records"
    exit 1
fi
