//! tonic gRPC server exposing the matching engine for synchronous queries.
//!
//! The engine binary supplies an [`EngineHandle`] implementation (typically an
//! `Arc<tokio::sync::Mutex<MatchingCore>>` wrapper) and the service impl reads
//! / mutates state through that trait — keeps this crate decoupled from the
//! binary's concrete state struct.

#![deny(unsafe_code)]

mod service;

pub use service::{
    spawn, EngineHandle, EngineOrderStatus, EngineStats, GrpcConfig, MatchingEngineServiceImpl,
    OrderbookView, ServerError,
};

/// Generated protobuf + tonic types under `hft.matching.v1`.
pub mod proto {
    tonic::include_proto!("hft.matching.v1");
}
