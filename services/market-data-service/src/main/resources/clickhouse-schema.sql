-- ClickHouse schema for market data. Run once at provisioning.

CREATE TABLE IF NOT EXISTS trades (
    trade_id      String,
    symbol        LowCardinality(String),
    price         Decimal(18, 8),
    quantity      Decimal(18, 8),
    buy_order_id  String,
    sell_order_id String,
    sequence      UInt64,
    executed_at   DateTime64(3, 'UTC')
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(executed_at)
ORDER BY (symbol, executed_at);

CREATE TABLE IF NOT EXISTS candles (
    symbol     LowCardinality(String),
    interval   LowCardinality(String),   -- 1m,5m,15m,1h,1d
    open_time  DateTime64(3, 'UTC'),
    open       Decimal(18, 8),
    high       Decimal(18, 8),
    low        Decimal(18, 8),
    close      Decimal(18, 8),
    volume     Decimal(28, 8),
    trade_count UInt32
) ENGINE = ReplacingMergeTree()
PARTITION BY (symbol, interval)
ORDER BY (symbol, interval, open_time);
