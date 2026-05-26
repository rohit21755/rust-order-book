//! Redis snapshot persistence for the matching engine.
//!
//! Keys:
//! - `orderbook:{symbol}` — JSON-serialized [`OrderBookSnapshot`], TTL 60s
//!
//! Cadence is controlled by the engine binary (every 100ms OR every 1000 events).

#![deny(unsafe_code)]

use orderbook::OrderBookSnapshot;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use std::time::Duration;
use thiserror::Error;
use tracing::{debug, warn};

/// Crate error type.
#[derive(Debug, Error)]
pub enum RedisError {
    /// Underlying redis-rs error.
    #[error("redis: {0}")]
    Redis(#[from] redis::RedisError),
    /// JSON error.
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

/// Result alias.
pub type RedisResult<T> = std::result::Result<T, RedisError>;

/// Snapshot client.
#[derive(Clone)]
pub struct SnapshotClient {
    conn: ConnectionManager,
    ttl: Duration,
}

impl SnapshotClient {
    /// Connect to Redis. `url` form: `redis://host:port/db`.
    pub async fn connect(url: &str, ttl: Duration) -> RedisResult<Self> {
        let client = redis::Client::open(url)?;
        let conn = ConnectionManager::new(client).await?;
        Ok(Self { conn, ttl })
    }

    fn key(symbol: &str) -> String {
        format!("orderbook:{symbol}")
    }

    /// Persist a snapshot (overwrites, refreshes TTL).
    pub async fn write(&mut self, snap: &OrderBookSnapshot) -> RedisResult<()> {
        let payload = serde_json::to_vec(snap)?;
        let key = Self::key(&snap.symbol);
        let ttl_secs = self.ttl.as_secs().max(1) as u64;
        // SET key value EX ttl — atomic w/ TTL refresh.
        let _: () = self.conn.set_ex(&key, payload, ttl_secs).await?;
        debug!(symbol = %snap.symbol, seq = snap.sequence, "snapshot written");
        Ok(())
    }

    /// Read latest snapshot for `symbol` if present.
    pub async fn read(&mut self, symbol: &str) -> RedisResult<Option<OrderBookSnapshot>> {
        let key = Self::key(symbol);
        let bytes: Option<Vec<u8>> = self.conn.get(&key).await?;
        match bytes {
            Some(b) => match serde_json::from_slice::<OrderBookSnapshot>(&b) {
                Ok(s) => Ok(Some(s)),
                Err(e) => {
                    warn!(error = %e, symbol = %symbol, "snapshot parse failed");
                    Ok(None)
                }
            },
            None => Ok(None),
        }
    }

    /// Delete a snapshot.
    pub async fn delete(&mut self, symbol: &str) -> RedisResult<()> {
        let key = Self::key(symbol);
        let _: () = self.conn.del(&key).await?;
        Ok(())
    }
}
