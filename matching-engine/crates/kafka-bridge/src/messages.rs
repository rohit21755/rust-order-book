//! JSON DTOs matching the shapes produced by Java services (shared-kafka-lib).

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Order side over the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum OrderSideMsg {
    Buy,
    Sell,
}

/// Order type over the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderTypeMsg {
    Limit,
    Market,
    StopLoss,
}

/// Event type over the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum OrderEventType {
    New,
    Cancel,
    Modify,
}

/// Wire-format order event consumed from Kafka.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderEventMsg {
    pub event_type: OrderEventType,
    pub order_id: Uuid,
    pub user_id: Uuid,
    pub symbol: String,
    pub side: OrderSideMsg,
    pub order_type: OrderTypeMsg,
    #[serde(default, with = "rust_decimal::serde::str_option")]
    pub price: Option<Decimal>,
    #[serde(default, with = "rust_decimal::serde::str_option")]
    pub stop_price: Option<Decimal>,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    pub idempotency_key: Option<String>,
    pub timestamp: Option<String>,
}

/// Wire-format trade event published to Kafka.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TradeEventMsg {
    pub trade_id: Uuid,
    pub buy_order_id: Uuid,
    pub sell_order_id: Uuid,
    pub buyer_user_id: Uuid,
    pub seller_user_id: Uuid,
    pub symbol: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    pub executed_at_ms: i64,
    pub sequence: u64,
}

impl OrderSideMsg {
    /// Convert wire type to engine type.
    pub fn into_engine(self) -> orderbook::Side {
        match self {
            OrderSideMsg::Buy => orderbook::Side::Buy,
            OrderSideMsg::Sell => orderbook::Side::Sell,
        }
    }
}

impl OrderTypeMsg {
    /// Convert wire type to engine type.
    pub fn into_engine(self) -> orderbook::OrderType {
        match self {
            OrderTypeMsg::Limit => orderbook::OrderType::Limit,
            OrderTypeMsg::Market => orderbook::OrderType::Market,
            OrderTypeMsg::StopLoss => orderbook::OrderType::StopLoss,
        }
    }
}

impl From<&orderbook::Trade> for TradeEventMsg {
    fn from(t: &orderbook::Trade) -> Self {
        Self {
            trade_id: t.trade_id,
            buy_order_id: t.buy_order_id,
            sell_order_id: t.sell_order_id,
            buyer_user_id: t.buyer_user_id,
            seller_user_id: t.seller_user_id,
            symbol: t.symbol.clone(),
            price: t.price,
            quantity: t.quantity,
            executed_at_ms: t.executed_at_ms,
            sequence: t.sequence,
        }
    }
}
