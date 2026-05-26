//! HFT matching engine entry point.
//!
//! Orchestration loop:
//!   Kafka order consumer → MatchingCore → batched commit (Kafka producer tx + Redis snapshot)
//!
//! Snapshots flushed every 100ms OR every 1000 processed events, whichever comes first.

#![deny(unsafe_code)]

mod config_loader;
mod engine;
mod grpc_handle;
mod metrics;
mod metrics_server;

use anyhow::Context;
use kafka_bridge::{OrderConsumer, OrderConsumerConfig, TradePublisher, TradePublisherConfig};
use redis_bridge::SnapshotClient;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{error, info};

use engine::MatchingCore;
use grpc_handle::CoreHandle;
use grpc_server::{spawn as spawn_grpc, GrpcConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let cfg = config_loader::load().context("failed to load config")?;
    info!(?cfg, "starting matching engine");

    // Wire infra.
    let consumer = OrderConsumer::new(&OrderConsumerConfig {
        bootstrap_servers: cfg.kafka.bootstrap_servers.clone(),
        topic: cfg.kafka.orders_topic.clone(),
        group_id: cfg.kafka.consumer_group.clone(),
        read_committed: cfg.kafka.read_committed,
        session_timeout_ms: cfg.kafka.session_timeout_ms,
        fetch_max_bytes: cfg.kafka.fetch_max_bytes,
        auto_offset_reset: cfg.kafka.auto_offset_reset.clone(),
    })
    .context("kafka consumer init")?;

    let publisher = Arc::new(TradePublisher::new(&TradePublisherConfig {
        bootstrap_servers: cfg.kafka.bootstrap_servers.clone(),
        trades_topic: cfg.kafka.trades_topic.clone(),
        orderbook_topic: cfg.kafka.orderbook_topic.clone(),
        transactional_id: cfg.kafka.transactional_id.clone(),
        transaction_timeout_ms: cfg.kafka.transaction_timeout_ms,
        send_timeout_ms: cfg.kafka.send_timeout_ms,
    })?);

    let snapshot_client = SnapshotClient::connect(&cfg.redis.url, Duration::from_secs(cfg.redis.ttl_secs))
        .await
        .context("redis connect")?;

    let core = Arc::new(Mutex::new(MatchingCore::new(
        cfg.symbols.clone(),
        Arc::clone(&publisher),
        snapshot_client.clone(),
        Duration::from_millis(cfg.snapshot.interval_ms),
        cfg.snapshot.event_threshold,
    )));

    // Restore prior snapshots.
    {
        let mut guard = core.lock().await;
        let mut client = snapshot_client.clone();
        for sym in &cfg.symbols {
            match client.read(sym).await {
                Ok(Some(snap)) => {
                    guard.restore(sym, &snap);
                    info!(symbol = %sym, sequence = snap.sequence, "restored snapshot");
                }
                Ok(None) => info!(symbol = %sym, "no snapshot to restore"),
                Err(e) => error!(error = %e, symbol = %sym, "restore failed"),
            }
        }
    }

    let shutdown = Arc::new(AtomicBool::new(false));
    install_signal_handlers(Arc::clone(&shutdown));

    // Prometheus /metrics endpoint on :9091.
    let metrics_addr: std::net::SocketAddr = cfg.metrics.bind_addr.parse()
        .context("invalid metrics bind addr")?;
    let _metrics_jh = metrics_server::spawn(metrics_addr);

    // gRPC server for synchronous queries (orderbook lookup, health, cancel).
    {
        let handle = Arc::new(CoreHandle::new(Arc::clone(&core)));
        let grpc_cfg = GrpcConfig {
            bind_addr: cfg.grpc.bind_addr.parse().context("invalid grpc bind addr")?,
            tls_cert: cfg.grpc.tls_cert.clone(),
            tls_key: cfg.grpc.tls_key.clone(),
        };
        match spawn_grpc(grpc_cfg, handle) {
            Ok(jh) => {
                info!("gRPC server task spawned");
                tokio::spawn(async move {
                    if let Err(e) = jh.await {
                        error!(error = %e, "gRPC server task join failed");
                    }
                });
            }
            Err(e) => error!(error = %e, "failed to start gRPC server"),
        }
    }

    // Periodic snapshot flusher.
    {
        let core = Arc::clone(&core);
        let shutdown = Arc::clone(&shutdown);
        let interval = Duration::from_millis(cfg.snapshot.interval_ms);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            while !shutdown.load(Ordering::Relaxed) {
                tick.tick().await;
                let mut guard = core.lock().await;
                if let Err(e) = guard.flush_due_snapshots(true).await {
                    error!(error = %e, "snapshot flush failed");
                }
            }
        });
    }

    let core_for_consumer = Arc::clone(&core);
    let shutdown_for_consumer = Arc::clone(&shutdown);

    consumer
        .run(
            move |msg| {
                let core = Arc::clone(&core_for_consumer);
                async move {
                    let mut guard = core.lock().await;
                    guard.handle(msg).await
                }
            },
            move || shutdown_for_consumer.load(Ordering::Relaxed),
        )
        .await?;

    info!("matching engine shutting down");
    Ok(())
}

fn init_tracing() {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,matching_engine=debug"));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .json()
        .init();
}

fn install_signal_handlers(shutdown: Arc<AtomicBool>) {
    tokio::spawn(async move {
        let mut sigterm = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                error!(error = %e, "failed to install SIGTERM handler");
                return;
            }
        };
        let mut sigint = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()) {
            Ok(s) => s,
            Err(e) => {
                error!(error = %e, "failed to install SIGINT handler");
                return;
            }
        };
        tokio::select! {
            _ = sigterm.recv() => info!("SIGTERM received"),
            _ = sigint.recv()  => info!("SIGINT received"),
        }
        shutdown.store(true, Ordering::Relaxed);
    });
}
