//! Errors surfaced by the Kafka bridge.

use thiserror::Error;

/// Result alias.
pub type BridgeResult<T> = std::result::Result<T, BridgeError>;

/// Kafka bridge failure modes.
#[derive(Debug, Error)]
pub enum BridgeError {
    /// rdkafka client error.
    #[error("kafka client error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),

    /// JSON serialization/deserialization failure.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// Configuration invalid or missing required field.
    #[error("config error: {0}")]
    Config(String),

    /// Transaction commit / abort failed (RdKafka transactional API).
    #[error("transaction error: {0}")]
    Transaction(String),
}
