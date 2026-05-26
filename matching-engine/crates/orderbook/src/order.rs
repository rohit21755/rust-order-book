//! Order model.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Unique order identifier.
pub type OrderId = Uuid;

/// Unique user identifier.
pub type UserId = Uuid;

/// Side of an order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    /// Buy side.
    Buy,
    /// Sell side.
    Sell,
}

impl Side {
    /// Opposite side helper.
    #[inline]
    pub const fn opposite(self) -> Side {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

/// Order type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderType {
    /// Limit order with explicit price.
    Limit,
    /// Market order — match against best available.
    Market,
    /// Stop-loss — triggers when last price crosses stop_price.
    StopLoss,
}

/// An order resting on or entering the book.
///
/// `sequence` is assigned by the matching engine on entry and provides FIFO
/// ordering at the same price level (price-time priority).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    /// Unique order identifier.
    pub id: OrderId,
    /// Submitting user.
    pub user_id: UserId,
    /// Trading symbol (e.g. BTC-USDT).
    pub symbol: String,
    /// Buy or sell.
    pub side: Side,
    /// LIMIT / MARKET / STOP_LOSS.
    pub order_type: OrderType,
    /// Limit price (required for LIMIT; ignored for MARKET; used as trigger for STOP_LOSS optional).
    pub price: Option<Decimal>,
    /// Stop price for STOP_LOSS.
    pub stop_price: Option<Decimal>,
    /// Original requested quantity.
    pub quantity: Decimal,
    /// Remaining quantity (mutated as fills occur).
    pub remaining: Decimal,
    /// Insertion sequence number — engine-assigned, monotonic.
    pub sequence: u64,
    /// Wall-clock timestamp (ms since epoch).
    pub timestamp_ms: i64,
}

impl Order {
    /// Build a new order with `remaining = quantity`.
    pub fn new(
        id: OrderId,
        user_id: UserId,
        symbol: impl Into<String>,
        side: Side,
        order_type: OrderType,
        price: Option<Decimal>,
        stop_price: Option<Decimal>,
        quantity: Decimal,
        timestamp_ms: i64,
    ) -> Self {
        Self {
            id,
            user_id,
            symbol: symbol.into(),
            side,
            order_type,
            price,
            stop_price,
            quantity,
            remaining: quantity,
            sequence: 0,
            timestamp_ms,
        }
    }

    /// `true` once the order has been fully filled.
    #[inline]
    pub fn is_filled(&self) -> bool {
        self.remaining.is_zero()
    }
}
