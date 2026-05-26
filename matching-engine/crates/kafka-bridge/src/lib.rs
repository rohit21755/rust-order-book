//! Async Kafka I/O for the matching engine.
//!
//! - [`OrderConsumer`] subscribes to the `orders` topic, deserializes JSON
//!   `OrderEvent`s (compatible with shared-kafka-lib's payload), and yields
//!   them to a caller-provided handler.
//! - [`TradePublisher`] writes `TradeEvent`s and `OrderBookSnapshot`s using a
//!   transactional rdkafka producer (exactly-once).

#![deny(unsafe_code)]

pub mod consumer;
pub mod error;
pub mod messages;
pub mod producer;

pub use consumer::{OrderConsumer, OrderConsumerConfig};
pub use error::{BridgeError, BridgeResult};
pub use messages::{OrderEventMsg, OrderEventType, OrderSideMsg, OrderTypeMsg};
pub use producer::{TradePublisher, TradePublisherConfig};
