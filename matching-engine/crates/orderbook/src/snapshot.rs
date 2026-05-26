//! Snapshot DTOs for persistence and market data distribution.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// One aggregated price level (price + total resting quantity).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Level {
    /// Price.
    pub price: Decimal,
    /// Aggregated quantity at this price.
    pub quantity: Decimal,
}

/// Top-N orderbook snapshot, suitable for Redis or downstream consumers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrderBookSnapshot {
    /// Trading symbol.
    pub symbol: String,
    /// Engine-monotonic sequence number.
    pub sequence: u64,
    /// Bid levels, highest price first.
    pub bids: Vec<Level>,
    /// Ask levels, lowest price first.
    pub asks: Vec<Level>,
    /// Wall-clock ms.
    pub timestamp_ms: i64,
}
