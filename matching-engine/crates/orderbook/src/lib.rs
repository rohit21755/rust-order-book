//! Limit order book + matching engine core.
//!
//! Design goals: deterministic execution, no I/O, zero allocation in the
//! hot path after warm-up. All price arithmetic uses [`rust_decimal::Decimal`].
//!
//! No `unsafe` code anywhere in this crate.

#![deny(unsafe_code)]
#![warn(missing_docs)]

mod book;
mod error;
mod order;
mod price_level;
mod snapshot;
mod stop_orders;
mod trade;

pub use book::{MatchResult, OrderBook};
pub use error::{BookError, BookResult};
pub use order::{Order, OrderId, OrderType, Side, UserId};
pub use price_level::PriceLevel;
pub use snapshot::{Level as SnapshotLevel, OrderBookSnapshot};
pub use stop_orders::StopOrderStore;
pub use trade::Trade;

/// Maximum levels included in a snapshot per side.
pub const SNAPSHOT_DEPTH: usize = 20;
