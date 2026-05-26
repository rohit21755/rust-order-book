//! Engine configuration: env vars + optional config file.

use config::{Config, Environment, File};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct EngineConfig {
    pub symbols: Vec<String>,
    pub kafka: KafkaConfig,
    pub redis: RedisConfig,
    pub snapshot: SnapshotConfig,
    pub grpc: GrpcConfig,
    pub metrics: MetricsConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetricsConfig {
    pub bind_addr: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GrpcConfig {
    pub bind_addr: String,
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub orders_topic: String,
    pub trades_topic: String,
    pub orderbook_topic: String,
    pub consumer_group: String,
    pub transactional_id: String,
    pub read_committed: bool,
    pub session_timeout_ms: u32,
    pub fetch_max_bytes: u32,
    pub auto_offset_reset: String,
    pub transaction_timeout_ms: u32,
    pub send_timeout_ms: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RedisConfig {
    pub url: String,
    pub ttl_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SnapshotConfig {
    pub interval_ms: u64,
    pub event_threshold: u64,
}

pub fn load() -> anyhow::Result<EngineConfig> {
    let cfg = Config::builder()
        .set_default("symbols", vec!["BTC-USDT", "ETH-USDT", "SOL-USDT"])?
        .set_default("kafka.bootstrap_servers", "localhost:29092")?
        .set_default("kafka.orders_topic", "orders")?
        .set_default("kafka.trades_topic", "trades")?
        .set_default("kafka.orderbook_topic", "orderbook-updates")?
        .set_default("kafka.consumer_group", "matching-engine-orders-group")?
        .set_default("kafka.transactional_id", "matching-engine-tx-1")?
        .set_default("kafka.read_committed", true)?
        .set_default("kafka.session_timeout_ms", 10_000)?
        .set_default("kafka.fetch_max_bytes", 1_048_576)?
        .set_default("kafka.auto_offset_reset", "earliest")?
        .set_default("kafka.transaction_timeout_ms", 30_000)?
        .set_default("kafka.send_timeout_ms", 5_000)?
        .set_default("redis.url", "redis://localhost:6379")?
        .set_default("redis.ttl_secs", 60)?
        .set_default("snapshot.interval_ms", 100)?
        .set_default("snapshot.event_threshold", 1000)?
        .set_default("grpc.bind_addr", "0.0.0.0:50051")?
        .set_default::<&str, Option<String>>("grpc.tls_cert", None)?
        .set_default::<&str, Option<String>>("grpc.tls_key", None)?
        .set_default("metrics.bind_addr", "0.0.0.0:9091")?
        .add_source(File::with_name("matching-engine").required(false))
        .add_source(Environment::with_prefix("MATCHING").separator("__"))
        .build()?;

    let cfg: EngineConfig = cfg.try_deserialize()?;
    Ok(cfg)
}
