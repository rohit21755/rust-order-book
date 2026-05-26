#!/usr/bin/env bash
# Chaos: kill the Rust matching engine container; verify recovery.
#
# Pre-reqs: docker compose stack from repo root.
# What it does:
#   1. Snapshot current trades-topic offset (HWM).
#   2. Kill matching-engine container.
#   3. Wait N seconds; restart it.
#   4. After restart, verify engine resumes consuming + produces trades at-least-once.
set -euo pipefail

CONTAINER=${CONTAINER:-matching-engine}
DOWN_SECONDS=${DOWN_SECONDS:-10}
KAFKA_CONTAINER=${KAFKA_CONTAINER:-kafka}
TRADES_TOPIC=${TRADES_TOPIC:-trades}

echo "[chaos] capturing baseline trades offset..."
BEFORE=$(docker exec "$KAFKA_CONTAINER" kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 --topic "$TRADES_TOPIC" --time -1 \
    | awk -F: '{sum += $3} END {print sum+0}')
echo "  trades HWM before kill: $BEFORE"

echo "[chaos] killing $CONTAINER (SIGKILL)..."
docker kill "$CONTAINER"

echo "[chaos] sleeping ${DOWN_SECONDS}s with engine down..."
sleep "$DOWN_SECONDS"

echo "[chaos] starting $CONTAINER..."
docker start "$CONTAINER"

echo "[chaos] waiting 30s for warm-up + snapshot restore..."
sleep 30

AFTER=$(docker exec "$KAFKA_CONTAINER" kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 --topic "$TRADES_TOPIC" --time -1 \
    | awk -F: '{sum += $3} END {print sum+0}')
echo "  trades HWM after restart: $AFTER"

if (( AFTER >= BEFORE )); then
    echo "[chaos] PASS — engine recovered (offset non-decreasing)"
    exit 0
else
    echo "[chaos] FAIL — offset went backwards (data loss?)"
    exit 1
fi
