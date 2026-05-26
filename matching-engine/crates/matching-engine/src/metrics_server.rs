//! Tiny Hyper-based HTTP server exposing Prometheus `/metrics` text format on :9091.

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use prometheus::{Encoder, TextEncoder};
use std::convert::Infallible;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::{error, info, warn};

/// Spawn the metrics endpoint as a background tokio task.
pub fn spawn(addr: SocketAddr) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let listener = match TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                error!(error = %e, addr = %addr, "metrics listener bind failed");
                return;
            }
        };
        info!(addr = %addr, "metrics endpoint listening on /metrics");
        loop {
            let (stream, _peer) = match listener.accept().await {
                Ok(p) => p,
                Err(e) => {
                    warn!(error = %e, "metrics accept failed");
                    continue;
                }
            };
            let io = TokioIo::new(stream);
            tokio::spawn(async move {
                if let Err(e) = http1::Builder::new()
                    .serve_connection(io, service_fn(handle))
                    .await
                {
                    warn!(error = %e, "metrics connection error");
                }
            });
        }
    })
}

async fn handle(req: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    match (req.method(), req.uri().path()) {
        (&hyper::Method::GET, "/metrics") => Ok(metrics_response()),
        (&hyper::Method::GET, "/healthz") => Ok(text(StatusCode::OK, "ok")),
        _ => Ok(text(StatusCode::NOT_FOUND, "not found")),
    }
}

fn metrics_response() -> Response<Full<Bytes>> {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buf = Vec::with_capacity(8 * 1024);
    if let Err(e) = encoder.encode(&metric_families, &mut buf) {
        warn!(error = %e, "metrics encode failed");
        return text(StatusCode::INTERNAL_SERVER_ERROR, "encode failed");
    }
    Response::builder()
        .status(StatusCode::OK)
        .header(hyper::header::CONTENT_TYPE, encoder.format_type())
        .body(Full::new(Bytes::from(buf)))
        .unwrap_or_else(|_| text(StatusCode::INTERNAL_SERVER_ERROR, "build failed"))
}

fn text(status: StatusCode, body: &'static str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Full::new(Bytes::from_static(body.as_bytes())))
        .unwrap()
}
