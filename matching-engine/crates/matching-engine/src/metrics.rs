//! Prometheus metrics registered lazily on first access.

use prometheus::{
    register_gauge_vec, register_histogram, register_int_counter, register_int_counter_vec,
    register_histogram_vec, GaugeVec, Histogram, HistogramVec, IntCounter, IntCounterVec,
};
use std::sync::OnceLock;

static ORDERS_PROCESSED: OnceLock<IntCounterVec> = OnceLock::new();
static MATCH_LATENCY: OnceLock<HistogramVec> = OnceLock::new();
static MATCH_LATENCY_US: OnceLock<Histogram> = OnceLock::new();
static SNAPSHOTS: OnceLock<IntCounterVec> = OnceLock::new();
static TRADES: OnceLock<IntCounterVec> = OnceLock::new();
static REJECTS: OnceLock<IntCounterVec> = OnceLock::new();
static ORDERS_MATCHED: OnceLock<IntCounter> = OnceLock::new();
static TRADES_EXECUTED: OnceLock<IntCounter> = OnceLock::new();
static ORDERBOOK_DEPTH: OnceLock<GaugeVec> = OnceLock::new();

pub fn orders_processed() -> &'static IntCounterVec {
    ORDERS_PROCESSED.get_or_init(|| {
        register_int_counter_vec!(
            "engine_orders_processed_total",
            "Total orders processed",
            &["symbol", "type"]
        )
        .expect("register orders_processed_total")
    })
}

pub fn match_latency_seconds() -> &'static HistogramVec {
    MATCH_LATENCY.get_or_init(|| {
        register_histogram_vec!(
            "engine_match_latency_seconds",
            "Per-order match latency",
            &["symbol"],
            vec![1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2]
        )
        .expect("register match_latency_seconds")
    })
}

pub fn snapshots() -> &'static IntCounterVec {
    SNAPSHOTS.get_or_init(|| {
        register_int_counter_vec!("engine_snapshots_total", "Snapshots written", &["symbol"])
            .expect("register snapshots_total")
    })
}

pub fn trades() -> &'static IntCounterVec {
    TRADES.get_or_init(|| {
        register_int_counter_vec!("engine_trades_total", "Trades emitted", &["symbol"])
            .expect("register trades_total")
    })
}

pub fn rejects() -> &'static IntCounterVec {
    REJECTS.get_or_init(|| {
        register_int_counter_vec!("engine_rejects_total", "Orders rejected", &["reason"])
            .expect("register rejects_total")
    })
}

pub fn orders_matched() -> &'static IntCounter {
    ORDERS_MATCHED.get_or_init(|| {
        register_int_counter!("orders_matched_total", "Orders matched by engine")
            .expect("register orders_matched_total")
    })
}

pub fn trades_executed() -> &'static IntCounter {
    TRADES_EXECUTED.get_or_init(|| {
        register_int_counter!("trades_executed_total", "Trades emitted by engine")
            .expect("register trades_executed_total")
    })
}

pub fn orderbook_depth() -> &'static GaugeVec {
    ORDERBOOK_DEPTH.get_or_init(|| {
        register_gauge_vec!(
            "orderbook_depth_gauge",
            "Current resting quantity at top-of-book per side",
            &["symbol", "side"]
        )
        .expect("register orderbook_depth_gauge")
    })
}

pub fn match_latency_microseconds() -> &'static Histogram {
    MATCH_LATENCY_US.get_or_init(|| {
        register_histogram!(
            "match_latency_microseconds",
            "Single-match latency in microseconds",
            vec![10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0, 10000.0]
        )
        .expect("register match_latency_microseconds")
    })
}
