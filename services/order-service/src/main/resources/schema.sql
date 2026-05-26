-- Order service schema (additive to global init.sql)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    side VARCHAR(10) NOT NULL,
    type VARCHAR(20) NOT NULL,
    price DECIMAL(18, 8),
    stop_price DECIMAL(18, 8),
    quantity DECIMAL(18, 8) NOT NULL,
    filled_quantity DECIMAL(18, 8) NOT NULL DEFAULT 0,
    avg_fill_price DECIMAL(18, 8),
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING_VALIDATION',
    idempotency_key VARCHAR(255) NOT NULL,
    reject_reason VARCHAR(512),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Backfill columns on pre-existing table.
ALTER TABLE orders ADD COLUMN IF NOT EXISTS stop_price DECIMAL(18, 8);
ALTER TABLE orders ADD COLUMN IF NOT EXISTS filled_quantity DECIMAL(18, 8) NOT NULL DEFAULT 0;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS avg_fill_price DECIMAL(18, 8);
ALTER TABLE orders ADD COLUMN IF NOT EXISTS reject_reason VARCHAR(512);
ALTER TABLE orders ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE orders ALTER COLUMN status TYPE VARCHAR(32);

-- Idempotency unique per user, not global, so collisions across users are fine.
DROP INDEX IF EXISTS orders_idempotency_key_key;
CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_user_idem ON orders(user_id, idempotency_key);

CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders(symbol);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_user_symbol_status ON orders(user_id, symbol, status);

-- ============================================================
-- Event Sourcing + CQRS (additive, idempotent)
-- ============================================================

-- Append-only event store. UNIQUE(aggregate_id, sequence_number) enforces monotonic seq.
CREATE TABLE IF NOT EXISTS event_store (
    event_id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aggregate_id     UUID NOT NULL,
    aggregate_type   VARCHAR(64) NOT NULL,
    event_type       VARCHAR(128) NOT NULL,
    sequence_number  BIGINT NOT NULL,
    payload          JSONB NOT NULL,
    metadata         JSONB NOT NULL DEFAULT '{}'::jsonb,
    "timestamp"      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_event_aggregate_seq UNIQUE (aggregate_id, sequence_number)
);
CREATE INDEX IF NOT EXISTS idx_event_aggregate ON event_store(aggregate_id, sequence_number);
CREATE INDEX IF NOT EXISTS idx_event_type_time ON event_store(aggregate_type, "timestamp");
CREATE INDEX IF NOT EXISTS idx_event_seq ON event_store(aggregate_type, event_id);

-- Append-only guard: deny UPDATE/DELETE via trigger.
CREATE OR REPLACE FUNCTION event_store_append_only() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'event_store is append-only';
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS trg_event_store_no_update ON event_store;
DROP TRIGGER IF EXISTS trg_event_store_no_delete ON event_store;
CREATE TRIGGER trg_event_store_no_update BEFORE UPDATE ON event_store
    FOR EACH ROW EXECUTE FUNCTION event_store_append_only();
CREATE TRIGGER trg_event_store_no_delete BEFORE DELETE ON event_store
    FOR EACH ROW EXECUTE FUNCTION event_store_append_only();

-- Snapshot per aggregate, taken every N events (default 50).
CREATE TABLE IF NOT EXISTS order_snapshots (
    aggregate_id     UUID PRIMARY KEY,
    version          BIGINT NOT NULL,
    snapshot_payload JSONB NOT NULL,
    "timestamp"      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Consumer-side idempotency ledger: insert per (consumer_group, event_id).
CREATE TABLE IF NOT EXISTS processed_events (
    consumer_group VARCHAR(128) NOT NULL,
    event_id       VARCHAR(128) NOT NULL,
    processed_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (consumer_group, event_id)
);
CREATE INDEX IF NOT EXISTS idx_processed_events_time ON processed_events(processed_at);

-- Denormalized read model (CQRS query side). Indexed for the common query patterns:
--  - by user
--  - by user + symbol
--  - by status
CREATE TABLE IF NOT EXISTS order_read_model (
    order_id         UUID PRIMARY KEY,
    user_id          UUID NOT NULL,
    symbol           VARCHAR(20) NOT NULL,
    side             VARCHAR(10) NOT NULL,
    type             VARCHAR(20) NOT NULL,
    price            DECIMAL(18, 8),
    stop_price       DECIMAL(18, 8),
    quantity         DECIMAL(18, 8) NOT NULL,
    filled_quantity  DECIMAL(18, 8) NOT NULL DEFAULT 0,
    avg_fill_price   DECIMAL(18, 8),
    status           VARCHAR(32) NOT NULL,
    idempotency_key  VARCHAR(255) NOT NULL,
    reject_reason    VARCHAR(512),
    last_sequence    BIGINT NOT NULL DEFAULT 0,
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at       TIMESTAMP WITH TIME ZONE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rm_user_created ON order_read_model(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_rm_user_symbol_status ON order_read_model(user_id, symbol, status);
CREATE INDEX IF NOT EXISTS idx_rm_status_created ON order_read_model(status, created_at DESC);
