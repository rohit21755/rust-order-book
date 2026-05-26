//! Implements `grpc_server::EngineHandle` against `MatchingCore`.

use crate::engine::{CancelOutcome, MatchingCore, OrderTrack, TrackState};
use async_trait::async_trait;
use grpc_server::{
    proto::{CancelResult, EngineOrderState},
    EngineHandle, EngineOrderStatus, EngineStats, OrderbookView, ServerError,
};
use orderbook::OrderId;
use rust_decimal::Decimal;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

pub struct CoreHandle {
    core: Arc<Mutex<MatchingCore>>,
}

impl CoreHandle {
    pub fn new(core: Arc<Mutex<MatchingCore>>) -> Self {
        Self { core }
    }
}

#[async_trait]
impl EngineHandle for CoreHandle {
    async fn snapshot(&self, symbol: &str, depth: u32) -> Result<OrderbookView, ServerError> {
        let guard = self.core.lock().await;
        let (snap, bid_counts, ask_counts) = guard
            .snapshot_view(symbol, depth)
            .ok_or_else(|| ServerError::SymbolNotFound(symbol.to_string()))?;
        Ok(OrderbookView { snapshot: snap, bid_counts, ask_counts })
    }

    async fn stats(&self) -> EngineStats {
        let guard = self.core.lock().await;
        EngineStats {
            orders_processed: guard.orders_processed,
            trades_executed: guard.trades_executed,
            uptime_ms: guard.started_at.elapsed().as_millis() as u64,
            version: env!("CARGO_PKG_VERSION").to_string(),
            serving: true,
        }
    }

    async fn order_status(
        &self,
        symbol: &str,
        order_id: OrderId,
    ) -> Result<EngineOrderStatus, ServerError> {
        let guard = self.core.lock().await;
        let track = guard.lookup_order(symbol, order_id);
        Ok(map_track(track))
    }

    async fn cancel(
        &self,
        symbol: &str,
        order_id: OrderId,
        _user_id: Uuid,
    ) -> Result<CancelResult, ServerError> {
        let mut guard = self.core.lock().await;
        let outcome = guard.cancel_by_id(symbol, order_id);
        Ok(match outcome {
            CancelOutcome::Cancelled => CancelResult::Cancelled,
            CancelOutcome::NotFound => CancelResult::NotFound,
            CancelOutcome::AlreadyTerminal => CancelResult::AlreadyTerminal,
        })
    }
}

fn map_track(track: Option<OrderTrack>) -> EngineOrderStatus {
    match track {
        None => EngineOrderStatus {
            state: EngineOrderState::OsNotFound,
            remaining_quantity: Decimal::ZERO,
            filled_quantity: Decimal::ZERO,
            avg_fill_price: None,
        },
        Some(t) => {
            let state = match t.state {
                TrackState::Resting => EngineOrderState::OsResting,
                TrackState::PartialFill => EngineOrderState::OsPartialFill,
                TrackState::Filled => EngineOrderState::OsFilled,
                TrackState::Cancelled => EngineOrderState::OsCancelled,
                TrackState::Rejected => EngineOrderState::OsRejected,
            };
            EngineOrderStatus {
                state,
                remaining_quantity: t.original_quantity - t.filled_quantity,
                filled_quantity: t.filled_quantity,
                avg_fill_price: t.avg_fill_price,
            }
        }
    }
}
