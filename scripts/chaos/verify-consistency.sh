#!/usr/bin/env bash
# Consistency probe: Kafka `trades` topic count should match the row count in
# portfolio_trades (DB-side ledger of trades the portfolio service settled).
#
# Drift = trades emitted by engine that have not yet been settled. After the
# system reaches steady state the two numbers must converge.
set -euo pipefail

KAFKA_CONTAINER=${KAFKA_CONTAINER:-kafka}
PG_CONTAINER=${PG_CONTAINER:-postgres}
PG_USER=${PG_USER:-hft}
PG_DB=${PG_DB:-hft}
TRADES_TOPIC=${TRADES_TOPIC:-trades}

KAFKA_COUNT=$(docker exec "$KAFKA_CONTAINER" kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 --topic "$TRADES_TOPIC" --time -1 \
    | awk -F: '{sum += $3} END {print sum+0}')

DB_COUNT=$(docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -tAc \
    "SELECT COUNT(DISTINCT trade_id) FROM portfolio_trades;")

echo "[verify] kafka trades count : $KAFKA_COUNT"
echo "[verify] db trades count    : $DB_COUNT"

DRIFT=$(( KAFKA_COUNT - DB_COUNT ))
echo "[verify] drift              : $DRIFT"

# Allow a small in-flight buffer.
if (( DRIFT >= 0 && DRIFT <= 10 )); then
    echo "[verify] PASS — drift within tolerance"
    exit 0
else
    echo "[verify] FAIL — drift outside tolerance"
    exit 1
fi
