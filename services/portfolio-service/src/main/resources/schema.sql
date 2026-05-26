-- Portfolio service schema (own tables; additive, idempotent).
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS portfolio_holdings (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id       UUID NOT NULL,
    symbol        VARCHAR(20) NOT NULL,
    quantity      DECIMAL(28, 8) NOT NULL DEFAULT 0,
    avg_buy_price DECIMAL(28, 8) NOT NULL DEFAULT 0,
    version       BIGINT NOT NULL DEFAULT 0,
    updated_at    TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_holding_user_symbol UNIQUE (user_id, symbol),
    CONSTRAINT chk_qty_non_negative CHECK (quantity >= 0)
);
CREATE INDEX IF NOT EXISTS idx_holdings_user ON portfolio_holdings(user_id);

CREATE TABLE IF NOT EXISTS portfolio_pnl_records (
    id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id        UUID NOT NULL,
    symbol         VARCHAR(20) NOT NULL,
    realized_pnl   DECIMAL(28, 8) NOT NULL DEFAULT 0,
    unrealized_pnl DECIMAL(28, 8) NOT NULL DEFAULT 0,
    timestamp      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_pnl_user_time ON portfolio_pnl_records(user_id, timestamp);

-- Exactly-once: a trade is settled at most once.
CREATE TABLE IF NOT EXISTS processed_trades (
    trade_id     VARCHAR(64) PRIMARY KEY,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS portfolio_trades (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id     UUID NOT NULL,
    trade_id    VARCHAR(64) NOT NULL,
    symbol      VARCHAR(20) NOT NULL,
    side        VARCHAR(4)  NOT NULL,   -- BUY / SELL
    price       DECIMAL(28, 8) NOT NULL,
    quantity    DECIMAL(28, 8) NOT NULL,
    realized_pnl DECIMAL(28, 8) NOT NULL DEFAULT 0,
    executed_at TIMESTAMP WITH TIME ZONE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_user_trades_user_time ON portfolio_trades(user_id, executed_at);
