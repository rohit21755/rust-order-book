//! Integration tests for the gRPC service impl using a fake EngineHandle.
//! Exercises the tonic service methods directly (no socket).

use async_trait::async_trait;
use grpc_server::proto::matching_engine_service_server::MatchingEngineService;
use grpc_server::proto::{
    CancelOrderRequest, CancelResult, Empty, EngineOrderState, HealthStatus, OrderBookRequest,
    OrderStatusRequest,
};
use grpc_server::{
    EngineHandle, EngineOrderStatus, EngineStats, MatchingEngineServiceImpl, OrderbookView,
    ServerError,
};
use orderbook::{OrderBookSnapshot, OrderId, SnapshotLevel};
use rust_decimal_macros::dec;
use std::sync::Arc;
use tonic::Request;
use uuid::Uuid;

struct FakeEngine {
    known_symbol: String,
}

#[async_trait]
impl EngineHandle for FakeEngine {
    async fn snapshot(&self, symbol: &str, depth: u32) -> Result<OrderbookView, ServerError> {
        if symbol != self.known_symbol {
            return Err(ServerError::SymbolNotFound(symbol.to_string()));
        }
        let snap = OrderBookSnapshot {
            symbol: symbol.to_string(),
            sequence: 11,
            bids: vec![SnapshotLevel { price: dec!(100), quantity: dec!(2) }],
            asks: vec![SnapshotLevel { price: dec!(101), quantity: dec!(3) }],
            timestamp_ms: 1,
        };
        let _ = depth;
        Ok(OrderbookView {
            snapshot: snap,
            bid_counts: vec![1],
            ask_counts: vec![2],
        })
    }

    async fn stats(&self) -> EngineStats {
        EngineStats {
            orders_processed: 5,
            trades_executed: 2,
            uptime_ms: 1234,
            version: "test".into(),
            serving: true,
        }
    }

    async fn order_status(
        &self,
        _symbol: &str,
        _order_id: OrderId,
    ) -> Result<EngineOrderStatus, ServerError> {
        Ok(EngineOrderStatus {
            state: EngineOrderState::OsPartialFill,
            remaining_quantity: dec!(0.5),
            filled_quantity: dec!(0.5),
            avg_fill_price: Some(dec!(100)),
        })
    }

    async fn cancel(
        &self,
        _symbol: &str,
        _order_id: OrderId,
        _user_id: Uuid,
    ) -> Result<CancelResult, ServerError> {
        Ok(CancelResult::Cancelled)
    }
}

fn svc() -> MatchingEngineServiceImpl {
    MatchingEngineServiceImpl::new(Arc::new(FakeEngine {
        known_symbol: "BTC-USDT".into(),
    }))
}

#[tokio::test]
async fn get_order_book_maps_levels() {
    let resp = svc()
        .get_order_book(Request::new(OrderBookRequest {
            symbol: "BTC-USDT".into(),
            depth: 10,
        }))
        .await
        .expect("ok")
        .into_inner();
    assert_eq!(resp.symbol, "BTC-USDT");
    assert_eq!(resp.sequence, 11);
    assert_eq!(resp.bids.len(), 1);
    assert_eq!(resp.bids[0].price, "100");
    assert_eq!(resp.bids[0].order_count, 1);
    assert_eq!(resp.asks[0].order_count, 2);
}

#[tokio::test]
async fn get_order_book_unknown_symbol_is_not_found() {
    let status = svc()
        .get_order_book(Request::new(OrderBookRequest {
            symbol: "DOGE-USDT".into(),
            depth: 0,
        }))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn get_order_book_empty_symbol_invalid() {
    let status = svc()
        .get_order_book(Request::new(OrderBookRequest {
            symbol: "".into(),
            depth: 0,
        }))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn health_reports_serving() {
    let resp = svc()
        .get_engine_health(Request::new(Empty {}))
        .await
        .expect("ok")
        .into_inner();
    assert_eq!(resp.status, HealthStatus::Serving as i32);
    assert_eq!(resp.orders_processed, 5);
    assert_eq!(resp.trades_executed, 2);
}

#[tokio::test]
async fn order_status_maps_state() {
    let resp = svc()
        .get_order_status(Request::new(OrderStatusRequest {
            order_id: Uuid::new_v4().to_string(),
            symbol: "BTC-USDT".into(),
        }))
        .await
        .expect("ok")
        .into_inner();
    assert_eq!(resp.state, EngineOrderState::OsPartialFill as i32);
    assert_eq!(resp.remaining_quantity, "0.5");
}

#[tokio::test]
async fn cancel_returns_cancelled() {
    let resp = svc()
        .cancel_order(Request::new(CancelOrderRequest {
            order_id: Uuid::new_v4().to_string(),
            symbol: "BTC-USDT".into(),
            user_id: Uuid::new_v4().to_string(),
        }))
        .await
        .expect("ok")
        .into_inner();
    assert_eq!(resp.result, CancelResult::Cancelled as i32);
}

#[tokio::test]
async fn cancel_rejects_bad_uuid() {
    let status = svc()
        .cancel_order(Request::new(CancelOrderRequest {
            order_id: "not-a-uuid".into(),
            symbol: "BTC-USDT".into(),
            user_id: Uuid::new_v4().to_string(),
        }))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}
