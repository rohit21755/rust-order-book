//! Trade execution record.

use crate::order::{OrderId, UserId};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Resulting trade when two orders cross.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    /// Globally unique trade id.
    pub trade_id: Uuid,
    /// Order id of the buy side.
    pub buy_order_id: OrderId,
    /// Order id of the sell side.
    pub sell_order_id: OrderId,
    /// Buyer user.
    pub buyer_user_id: UserId,
    /// Seller user.
    pub seller_user_id: UserId,
    /// Trading symbol.
    pub symbol: String,
    /// Execution price.
    pub price: Decimal,
    /// Executed quantity.
    pub quantity: Decimal,
    /// Wall-clock timestamp (ms).
    pub executed_at_ms: i64,
    /// Per-symbol monotonic sequence number assigned by the engine.
    pub sequence: u64,
}
