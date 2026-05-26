//! Transactional trade publisher.

use crate::error::{BridgeError, BridgeResult};
use crate::messages::TradeEventMsg;
use orderbook::{OrderBookSnapshot, Trade};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use std::time::Duration;
use tracing::{debug, error};

/// Trade publisher config.
#[derive(Debug, Clone)]
pub struct TradePublisherConfig {
    pub bootstrap_servers: String,
    pub trades_topic: String,
    pub orderbook_topic: String,
    /// Stable transactional id — MUST persist across restarts to recover open transactions.
    pub transactional_id: String,
    /// Transaction commit timeout.
    pub transaction_timeout_ms: u32,
    pub send_timeout_ms: u32,
}

impl Default for TradePublisherConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: "localhost:29092".into(),
            trades_topic: "trades".into(),
            orderbook_topic: "orderbook-updates".into(),
            transactional_id: "matching-engine-tx".into(),
            transaction_timeout_ms: 30_000,
            send_timeout_ms: 5_000,
        }
    }
}

/// Transactional producer writing trades + orderbook snapshots atomically.
pub struct TradePublisher {
    producer: FutureProducer,
    trades_topic: String,
    orderbook_topic: String,
    send_timeout: Duration,
}

impl TradePublisher {
    /// Build + initialise the producer transactionally.
    pub fn new(cfg: &TradePublisherConfig) -> BridgeResult<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &cfg.bootstrap_servers)
            .set("enable.idempotence", "true")
            .set("transactional.id", &cfg.transactional_id)
            .set("acks", "all")
            .set("max.in.flight.requests.per.connection", "5")
            .set("compression.type", "lz4")
            .set("linger.ms", "5")
            .set("transaction.timeout.ms", cfg.transaction_timeout_ms.to_string())
            .create()?;

        producer
            .init_transactions(Timeout::After(Duration::from_millis(cfg.transaction_timeout_ms as u64)))
            .map_err(|e| BridgeError::Transaction(e.to_string()))?;

        Ok(Self {
            producer,
            trades_topic: cfg.trades_topic.clone(),
            orderbook_topic: cfg.orderbook_topic.clone(),
            send_timeout: Duration::from_millis(cfg.send_timeout_ms as u64),
        })
    }

    /// Atomically publish a batch of trades + optional snapshot.
    /// On any failure the transaction is aborted; the consumer will redeliver.
    pub async fn publish_batch(
        &self,
        trades: &[Trade],
        snapshot: Option<&OrderBookSnapshot>,
    ) -> BridgeResult<()> {
        self.producer
            .begin_transaction()
            .map_err(|e| BridgeError::Transaction(e.to_string()))?;

        let result = self.publish_inner(trades, snapshot).await;

        match result {
            Ok(()) => {
                self.producer
                    .commit_transaction(Timeout::After(self.send_timeout))
                    .map_err(|e| BridgeError::Transaction(format!("commit: {e}")))?;
                debug!(trades = trades.len(), "tx committed");
                Ok(())
            }
            Err(e) => {
                error!(error = %e, "tx aborting");
                let _ = self
                    .producer
                    .abort_transaction(Timeout::After(self.send_timeout));
                Err(e)
            }
        }
    }

    async fn publish_inner(
        &self,
        trades: &[Trade],
        snapshot: Option<&OrderBookSnapshot>,
    ) -> BridgeResult<()> {
        for t in trades {
            let payload = serde_json::to_vec(&TradeEventMsg::from(t))?;
            let key = t.symbol.clone();
            let record = FutureRecord::to(&self.trades_topic)
                .key(&key)
                .payload(&payload);
            self.producer
                .send(record, Timeout::After(self.send_timeout))
                .await
                .map_err(|(err, _)| BridgeError::Kafka(err))?;
        }

        if let Some(snap) = snapshot {
            let payload = serde_json::to_vec(snap)?;
            let key = snap.symbol.clone();
            let record = FutureRecord::to(&self.orderbook_topic)
                .key(&key)
                .payload(&payload);
            self.producer
                .send(record, Timeout::After(self.send_timeout))
                .await
                .map_err(|(err, _)| BridgeError::Kafka(err))?;
        }
        Ok(())
    }
}
