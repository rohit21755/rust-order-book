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
