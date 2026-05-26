//! Async order consumer using rdkafka StreamConsumer.

use crate::error::{BridgeError, BridgeResult};
use crate::messages::OrderEventMsg;
use futures::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::Message;
use std::time::Duration;
use tracing::{debug, error, warn};

/// Consumer configuration. Keep mirror of `hft.kafka.*` semantics.
#[derive(Debug, Clone)]
pub struct OrderConsumerConfig {
    /// Comma-separated broker list.
    pub bootstrap_servers: String,
    /// Topic to subscribe to (typically `orders`).
    pub topic: String,
    /// Consumer group id (spec mandates `matching-engine-orders-group`).
    pub group_id: String,
    /// `true` to enable `isolation.level=read_committed` (paired with EOS producers upstream).
    pub read_committed: bool,
    /// Session timeout (ms).
    pub session_timeout_ms: u32,
    /// Fetch max bytes per poll.
    pub fetch_max_bytes: u32,
    /// `auto.offset.reset` value (`earliest` / `latest`).
    pub auto_offset_reset: String,
}

impl Default for OrderConsumerConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: "localhost:29092".into(),
            topic: "orders".into(),
            group_id: "matching-engine-orders-group".into(),
            read_committed: true,
            session_timeout_ms: 10_000,
            fetch_max_bytes: 1_048_576,
            auto_offset_reset: "earliest".into(),
        }
    }
}

/// Wraps an rdkafka StreamConsumer and yields `OrderEventMsg` records.
pub struct OrderConsumer {
    consumer: StreamConsumer,
    topic: String,
}

impl OrderConsumer {
    /// Build + subscribe.
    pub fn new(cfg: &OrderConsumerConfig) -> BridgeResult<Self> {
        let mut client = ClientConfig::new();
        client
            .set("bootstrap.servers", &cfg.bootstrap_servers)
            .set("group.id", &cfg.group_id)
            .set("enable.auto.commit", "false")
            .set("session.timeout.ms", cfg.session_timeout_ms.to_string())
            .set("fetch.max.bytes", cfg.fetch_max_bytes.to_string())
            .set("auto.offset.reset", &cfg.auto_offset_reset);

        if cfg.read_committed {
            client.set("isolation.level", "read_committed");
        }

        let consumer: StreamConsumer = client.create()?;
        consumer.subscribe(&[&cfg.topic])?;
        Ok(Self {
            consumer,
            topic: cfg.topic.clone(),
        })
    }

    /// Drive the message stream. Calls `handler` for each successfully-deserialized order;
    /// on handler success, commits the offset (at-least-once). Runs until `shutdown` returns true.
    pub async fn run<F, Fut>(&self, mut handler: F, shutdown: impl Fn() -> bool) -> BridgeResult<()>
    where
        F: FnMut(OrderEventMsg) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<()>>,
    {
        let mut stream = self.consumer.stream();
        while !shutdown() {
            let next = match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
                Ok(Some(Ok(m))) => m,
                Ok(Some(Err(e))) => {
                    error!(topic = %self.topic, error = %e, "kafka stream error");
                    continue;
                }
                Ok(None) => break,
                Err(_) => continue, // timeout — poll for shutdown
            };

            let payload = match next.payload() {
                Some(p) => p,
                None => {
                    warn!("empty payload, skipping");
                    let _ = self.consumer.commit_message(&next, CommitMode::Async);
                    continue;
                }
            };

            let msg: OrderEventMsg = match serde_json::from_slice(payload) {
                Ok(m) => m,
                Err(e) => {
                    error!(error = %e, "failed to deserialize OrderEventMsg; skipping");
                    let _ = self.consumer.commit_message(&next, CommitMode::Async);
                    continue;
                }
            };

            match handler(msg).await {
                Ok(()) => {
                    if let Err(e) = self.consumer.commit_message(&next, CommitMode::Async) {
                        error!(error = %e, "commit failed");
                    }
                }
                Err(e) => {
                    error!(error = %e, "handler returned error; NOT committing offset (will redeliver)");
                    return Err(BridgeError::Config(format!("handler failed: {e}")));
                }
            }

            debug!("processed kafka record");
        }
        Ok(())
    }
}
