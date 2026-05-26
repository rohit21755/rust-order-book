//! Error types surfaced by the orderbook crate.

use thiserror::Error;
use uuid::Uuid;

/// Result alias used throughout the crate.
pub type BookResult<T> = std::result::Result<T, BookError>;

/// All failure modes of order operations.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum BookError {
    /// Submitted order has non-positive quantity or non-positive limit price.
    #[error("invalid order: {0}")]
    InvalidOrder(String),

    /// Market order had no liquidity to fill against.
    #[error("no liquidity for MARKET order id={0}")]
    NoLiquidity(Uuid),

    /// Cancel referenced an order id that is not on the book.
    #[error("order not found: {0}")]
    OrderNotFound(Uuid),

    /// Self-trade prevention triggered.
    #[error("self-trade prevented for user {0}")]
    SelfTradePrevented(Uuid),

    /// Order belongs to a different symbol than the book.
    #[error("symbol mismatch: book={0} order={1}")]
    SymbolMismatch(String, String),
}
