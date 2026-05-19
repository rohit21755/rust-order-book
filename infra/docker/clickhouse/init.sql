CREATE DATABASE IF NOT EXISTS hft_db;

USE hft_db;

CREATE TABLE IF NOT EXISTS candles (
    symbol String,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    interval String,
    timestamp DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, interval, timestamp);

CREATE TABLE IF NOT EXISTS tick_data (
    symbol String,
    price Float64,
    quantity Float64,
    trade_id String,
    timestamp DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, timestamp);
