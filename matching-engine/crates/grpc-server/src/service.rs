//! gRPC service implementation backed by an injected [`EngineHandle`].

use crate::proto::matching_engine_service_server::{
    MatchingEngineService, MatchingEngineServiceServer,
};
use crate::proto::{
    CancelOrderRequest, CancelOrderResponse, CancelResult, EngineOrderState, Empty,
    HealthResponse, HealthStatus, OrderBookRequest, OrderBookResponse, OrderStatusRequest,
    OrderStatusResponse, PriceLevel,
};
use async_trait::async_trait;
use orderbook::{OrderBookSnapshot, OrderId};
use rust_decimal::Decimal;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;
use tokio::task::JoinHandle;
use tonic::transport::server::{Server, ServerTlsConfig};
use tonic::transport::Identity;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Snapshot-like view returned by the engine without holding the lock long.
#[derive(Debug, Clone)]
pub struct OrderbookView {
    pub snapshot: OrderBookSnapshot,
    /// Per-level order counts aligned with `snapshot.bids` and `snapshot.asks`.
    pub bid_counts: Vec<u32>,
    pub ask_counts: Vec<u32>,
}

/// Engine-wide counters for the health endpoint.
#[derive(Debug, Clone, Default)]
pub struct EngineStats {
    pub orders_processed: u64,
    pub trades_executed: u64,
    pub uptime_ms: u64,
    pub version: String,
    pub serving: bool,
}

/// Per-order status returned to gRPC clients.
#[derive(Debug, Clone)]
pub struct EngineOrderStatus {
    pub state: EngineOrderState,
    pub remaining_quantity: Decimal,
    pub filled_quantity: Decimal,
    pub avg_fill_price: Option<Decimal>,
}

/// Abstraction over the engine state used by the service impl.
///
/// Implementations live in the binary crate to avoid cyclic crate deps.
#[async_trait]
pub trait EngineHandle: Send + Sync + 'static {
    /// Top-N orderbook snapshot for `symbol`. `depth = 0` → engine default.
    async fn snapshot(&self, symbol: &str, depth: u32) -> Result<OrderbookView, ServerError>;

    /// Engine health + counters.
    async fn stats(&self) -> EngineStats;

    /// Order status by id.
    async fn order_status(
        &self,
        symbol: &str,
        order_id: OrderId,
    ) -> Result<EngineOrderStatus, ServerError>;

    /// Cancel an order in-engine.
    async fn cancel(
        &self,
        symbol: &str,
        order_id: OrderId,
        user_id: Uuid,
    ) -> Result<CancelResult, ServerError>;
}

/// Failure modes surfaced from the engine layer to gRPC `Status`.
#[derive(Debug, Error)]
pub enum ServerError {
    #[error("symbol not found: {0}")]
    SymbolNotFound(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("engine error: {0}")]
    Engine(String),
}

impl From<ServerError> for Status {
    fn from(e: ServerError) -> Self {
        match e {
            ServerError::SymbolNotFound(s) => Status::not_found(s),
            ServerError::InvalidArgument(s) => Status::invalid_argument(s),
            ServerError::Engine(s) => Status::internal(s),
        }
    }
}

/// gRPC server config.
#[derive(Debug, Clone)]
pub struct GrpcConfig {
    pub bind_addr: SocketAddr,
    /// Optional PEM-encoded cert + key paths. Both required for TLS.
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:50051".parse().expect("static addr"),
            tls_cert: None,
            tls_key: None,
        }
    }
}

/// `tonic` service impl. Wraps an `Arc<dyn EngineHandle>`.
pub struct MatchingEngineServiceImpl {
    handle: Arc<dyn EngineHandle>,
}

impl MatchingEngineServiceImpl {
    pub fn new(handle: Arc<dyn EngineHandle>) -> Self {
        Self { handle }
    }
}

#[async_trait]
impl MatchingEngineService for MatchingEngineServiceImpl {
    async fn get_order_book(
        &self,
        request: Request<OrderBookRequest>,
    ) -> Result<Response<OrderBookResponse>, Status> {
        let req = request.into_inner();
        if req.symbol.is_empty() {
            return Err(Status::invalid_argument("symbol required"));
        }
        let depth = if req.depth <= 0 { 20 } else { req.depth as u32 };
        let view = self.handle.snapshot(&req.symbol, depth).await?;
        Ok(Response::new(to_proto_book(&view)))
    }

    async fn get_engine_health(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<HealthResponse>, Status> {
        let s = self.handle.stats().await;
        let status = if s.serving {
            HealthStatus::Serving
        } else {
            HealthStatus::NotServing
        };
        Ok(Response::new(HealthResponse {
            status: status as i32,
            orders_processed: s.orders_processed,
            trades_executed: s.trades_executed,
            uptime_ms: s.uptime_ms,
            version: s.version,
        }))
    }

    async fn get_order_status(
        &self,
        request: Request<OrderStatusRequest>,
    ) -> Result<Response<OrderStatusResponse>, Status> {
        let req = request.into_inner();
        if req.symbol.is_empty() || req.order_id.is_empty() {
            return Err(Status::invalid_argument("symbol and order_id required"));
        }
        let order_id = Uuid::parse_str(&req.order_id)
            .map_err(|e| Status::invalid_argument(format!("bad uuid: {e}")))?;
        let st = self.handle.order_status(&req.symbol, order_id).await?;
        Ok(Response::new(OrderStatusResponse {
            order_id: req.order_id,
            symbol: req.symbol,
            state: st.state as i32,
            remaining_quantity: st.remaining_quantity.to_string(),
            filled_quantity: st.filled_quantity.to_string(),
            avg_fill_price: st
                .avg_fill_price
                .map(|d| d.to_string())
                .unwrap_or_default(),
        }))
    }

    async fn cancel_order(
        &self,
        request: Request<CancelOrderRequest>,
    ) -> Result<Response<CancelOrderResponse>, Status> {
        let req = request.into_inner();
        let order_id = Uuid::parse_str(&req.order_id)
            .map_err(|e| Status::invalid_argument(format!("bad order uuid: {e}")))?;
        let user_id = Uuid::parse_str(&req.user_id)
            .map_err(|e| Status::invalid_argument(format!("bad user uuid: {e}")))?;
        let result = self.handle.cancel(&req.symbol, order_id, user_id).await?;
        let message = match result {
            CancelResult::Cancelled => "ok",
            CancelResult::NotFound => "order not found",
            CancelResult::AlreadyTerminal => "order already terminal",
            CancelResult::Unknown => "unknown",
        }
        .to_string();
        Ok(Response::new(CancelOrderResponse {
            order_id: req.order_id,
            result: result as i32,
            message,
        }))
    }
}

fn to_proto_book(view: &OrderbookView) -> OrderBookResponse {
    let bids: Vec<PriceLevel> = view
        .snapshot
        .bids
        .iter()
        .enumerate()
        .map(|(i, lvl)| PriceLevel {
            price: lvl.price.to_string(),
            quantity: lvl.quantity.to_string(),
            order_count: view.bid_counts.get(i).copied().unwrap_or(0) as i32,
        })
        .collect();
    let asks: Vec<PriceLevel> = view
        .snapshot
        .asks
        .iter()
        .enumerate()
        .map(|(i, lvl)| PriceLevel {
            price: lvl.price.to_string(),
            quantity: lvl.quantity.to_string(),
            order_count: view.ask_counts.get(i).copied().unwrap_or(0) as i32,
        })
        .collect();
    OrderBookResponse {
        symbol: view.snapshot.symbol.clone(),
        bids,
        asks,
        sequence: view.snapshot.sequence,
        timestamp_ms: view.snapshot.timestamp_ms,
    }
}

/// Spawn the tonic server as a tokio task; returns the join handle.
///
/// On TLS: both `tls_cert` + `tls_key` must be present (PEM paths).
pub fn spawn(
    cfg: GrpcConfig,
    handle: Arc<dyn EngineHandle>,
) -> Result<JoinHandle<Result<(), tonic::transport::Error>>, anyhow::Error> {
    let svc = MatchingEngineServiceServer::new(MatchingEngineServiceImpl::new(handle));

    let server_fut: JoinHandle<Result<(), tonic::transport::Error>> = if let (Some(cert), Some(key)) =
        (cfg.tls_cert.as_ref(), cfg.tls_key.as_ref())
    {
        let cert_bytes = std::fs::read(Path::new(cert))?;
        let key_bytes = std::fs::read(Path::new(key))?;
        let identity = Identity::from_pem(cert_bytes, key_bytes);
        let tls = ServerTlsConfig::new().identity(identity);
        info!(addr = %cfg.bind_addr, "starting tonic server with TLS");
        let mut builder = Server::builder().tls_config(tls)?;
        let addr = cfg.bind_addr;
        tokio::spawn(async move { builder.add_service(svc).serve(addr).await })
    } else {
        warn!(addr = %cfg.bind_addr, "starting tonic server WITHOUT TLS (dev mode)");
        let addr = cfg.bind_addr;
        tokio::spawn(async move { Server::builder().add_service(svc).serve(addr).await })
    };

    debug!("gRPC server task spawned");
    Ok(server_fut)
}
