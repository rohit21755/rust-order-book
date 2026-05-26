//! Stateful matching core: owns one OrderBook + StopOrderStore per symbol.

use crate::metrics;
use indexmap::IndexMap;
use kafka_bridge::{OrderEventMsg, OrderEventType, TradePublisher};
use orderbook::{Order, OrderBook, OrderBookSnapshot, OrderId, OrderType, Side, StopOrderStore, Trade};
use redis_bridge::SnapshotClient;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

/// Per-symbol mutable state held by the engine.
struct SymbolState {
    book: OrderBook,
    stops: StopOrderStore,
    events_since_snapshot: u64,
    last_snapshot_at: Instant,
    /// Per-order history (used by gRPC GetOrderStatus). Unbounded; prune externally if needed.
    order_history: HashMap<OrderId, OrderTrack>,
}

#[derive(Debug, Clone)]
pub(crate) struct OrderTrack {
    pub state: TrackState,
    pub original_quantity: Decimal,
    pub filled_quantity: Decimal,
    pub avg_fill_price: Option<Decimal>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TrackState {
    Resting,
    PartialFill,
    Filled,
    Cancelled,
    Rejected,
}

/// Multi-symbol matching engine.
pub struct MatchingCore {
    symbols: IndexMap<String, SymbolState>,
    publisher: Arc<TradePublisher>,
    redis: SnapshotClient,
    snapshot_interval: Duration,
    event_threshold: u64,
    /// Cumulative counters (exposed via gRPC GetEngineHealth).
    pub orders_processed: u64,
    pub trades_executed: u64,
    pub started_at: Instant,
}

impl MatchingCore {
    /// Build state for the configured symbol list.
    pub fn new(
        symbols: Vec<String>,
        publisher: Arc<TradePublisher>,
        redis: SnapshotClient,
        snapshot_interval: Duration,
        event_threshold: u64,
    ) -> Self {
        let mut map = IndexMap::new();
        for s in symbols {
            map.insert(
                s.clone(),
                SymbolState {
                    book: OrderBook::new(s),
                    stops: StopOrderStore::new(),
                    events_since_snapshot: 0,
                    last_snapshot_at: Instant::now(),
                    order_history: HashMap::with_capacity(4096),
                },
            );
        }
        Self {
            symbols: map,
            publisher,
            redis,
            snapshot_interval,
            event_threshold,
            orders_processed: 0,
            trades_executed: 0,
            started_at: Instant::now(),
        }
    }

    /// Build orderbook view (snapshot + per-level order counts) for gRPC.
    pub fn snapshot_view(&self, symbol: &str, depth: u32) -> Option<(OrderBookSnapshot, Vec<u32>, Vec<u32>)> {
        let state = self.symbols.get(symbol)?;
        let mut snap = state.book.get_snapshot();
        let depth = depth as usize;
        if depth > 0 {
            snap.bids.truncate(depth);
            snap.asks.truncate(depth);
        }
        // Per-level counts derived from the engine-internal book traversal.
        let bid_counts = state.book.bid_counts(depth);
        let ask_counts = state.book.ask_counts(depth);
        Some((snap, bid_counts, ask_counts))
    }

    /// Lookup per-order tracking state for gRPC GetOrderStatus.
    pub fn lookup_order(&self, symbol: &str, order_id: OrderId) -> Option<OrderTrack> {
        self.symbols.get(symbol)?.order_history.get(&order_id).cloned()
    }

    /// Cancel by id+symbol invoked from gRPC.
    pub fn cancel_by_id(&mut self, symbol: &str, order_id: OrderId) -> CancelOutcome {
        let state = match self.symbols.get_mut(symbol) {
            Some(s) => s,
            None => return CancelOutcome::NotFound,
        };
        // Stop orders first.
        if state.stops.remove(&order_id).is_some() {
            state.order_history.insert(
                order_id,
                OrderTrack {
                    state: TrackState::Cancelled,
                    original_quantity: Decimal::ZERO,
                    filled_quantity: Decimal::ZERO,
                    avg_fill_price: None,
                },
            );
            return CancelOutcome::Cancelled;
        }
        match state.book.cancel_order(order_id) {
            Ok(o) => {
                let entry = state.order_history.entry(o.id).or_insert(OrderTrack {
                    state: TrackState::Resting,
                    original_quantity: o.quantity,
                    filled_quantity: o.quantity - o.remaining,
                    avg_fill_price: None,
                });
                entry.state = TrackState::Cancelled;
                CancelOutcome::Cancelled
            }
            Err(_) => {
                // If we know about the order in history but not on book → already terminal.
                match state.order_history.get(&order_id).map(|t| t.state) {
                    Some(TrackState::Filled)
                    | Some(TrackState::Cancelled)
                    | Some(TrackState::Rejected) => CancelOutcome::AlreadyTerminal,
                    _ => CancelOutcome::NotFound,
                }
            }
        }
    }

    /// Restore one symbol from a snapshot.
    pub fn restore(&mut self, symbol: &str, snap: &OrderBookSnapshot) {
        if let Some(state) = self.symbols.get_mut(symbol) {
            state.book.restore_from_snapshot(snap);
        }
    }

    /// Handle a single inbound order event.
    pub async fn handle(&mut self, msg: OrderEventMsg) -> anyhow::Result<()> {
        let start = Instant::now();
        let symbol = msg.symbol.clone();
        let labels = [symbol.as_str()];
        let _timer = metrics::match_latency_seconds()
            .with_label_values(&labels)
            .start_timer();

        let state = match self.symbols.get_mut(&symbol) {
            Some(s) => s,
            None => {
                warn!(%symbol, "unknown symbol; dropping order");
                metrics::rejects().with_label_values(&["UNKNOWN_SYMBOL"]).inc();
                return Ok(());
            }
        };

        let mut trades_emitted: u64 = 0;
        match msg.event_type {
            OrderEventType::New => {
                trades_emitted =
                    Self::process_new(&symbol, state, msg, &self.publisher).await?;
            }
            OrderEventType::Cancel => {
                Self::process_cancel(state, &msg);
            }
            OrderEventType::Modify => {
                warn!(order_id = %msg.order_id, "MODIFY not implemented; treating as cancel");
                Self::process_cancel(state, &msg);
            }
        }
        self.trades_executed = self.trades_executed.saturating_add(trades_emitted);

        metrics::orders_processed()
            .with_label_values(&[symbol.as_str(), &format!("{:?}", msg.order_type)])
            .inc();
        self.orders_processed = self.orders_processed.saturating_add(1);

        state.events_since_snapshot += 1;
        let elapsed = start.elapsed();
        debug!(?elapsed, "order handled");

        // Opportunistically flush snapshot when threshold met (cheap path).
        if state.events_since_snapshot >= self.event_threshold {
            self.flush_one(&symbol).await?;
        }
        Ok(())
    }

    async fn process_new(
        symbol: &str,
        state: &mut SymbolState,
        msg: OrderEventMsg,
        publisher: &TradePublisher,
    ) -> anyhow::Result<u64> {
        let order = match build_order(&msg) {
            Ok(o) => o,
            Err(e) => {
                error!(error = %e, "invalid order; rejecting");
                metrics::rejects().with_label_values(&["INVALID_ORDER"]).inc();
                state.order_history.insert(
                    msg.order_id,
                    OrderTrack {
                        state: TrackState::Rejected,
                        original_quantity: msg.quantity,
                        filled_quantity: Decimal::ZERO,
                        avg_fill_price: None,
                    },
                );
                return Ok(0);
            }
        };

        let order_id = order.id;
        let original_qty = order.quantity;

        // STOP_LOSS — park in stop store.
        if order.order_type == OrderType::StopLoss {
            state.stops.insert(order);
            state.order_history.insert(
                order_id,
                OrderTrack {
                    state: TrackState::Resting,
                    original_quantity: original_qty,
                    filled_quantity: Decimal::ZERO,
                    avg_fill_price: None,
                },
            );
            return Ok(0);
        }

        let mut trades_emitted = 0u64;
        match state.book.match_order(order) {
            Ok(res) => {
                let filled: Decimal = res.trades.iter().map(|t| t.quantity).sum();
                let avg = if filled > Decimal::ZERO {
                    let weighted: Decimal = res.trades.iter().map(|t| t.price * t.quantity).sum();
                    Some(weighted / filled)
                } else {
                    None
                };
                let st = if res.resting {
                    if filled > Decimal::ZERO { TrackState::PartialFill } else { TrackState::Resting }
                } else if filled >= original_qty {
                    TrackState::Filled
                } else if filled > Decimal::ZERO {
                    TrackState::Filled // MARKET partial: dropped remainder counts as filled
                } else {
                    TrackState::Rejected
                };
                state.order_history.insert(
                    order_id,
                    OrderTrack {
                        state: st,
                        original_quantity: original_qty,
                        filled_quantity: filled,
                        avg_fill_price: avg,
                    },
                );

                if !res.trades.is_empty() {
                    trades_emitted = res.trades.len() as u64;
                    metrics::trades()
                        .with_label_values(&[symbol])
                        .inc_by(trades_emitted);

                    // Atomic publish: trades + interim snapshot in a single Kafka transaction.
                    let snap = state.book.get_snapshot();
                    if let Err(e) = publisher.publish_batch(&res.trades, Some(&snap)).await {
                        error!(error = %e, "publish failed; downstream will redeliver");
                        // The Kafka consumer will NOT commit, so the event will replay.
                        return Err(anyhow::anyhow!("publish failed: {e}"));
                    }

                    // Mark maker counterparties.
                    for t in &res.trades {
                        let maker_id = if t.buy_order_id == order_id { t.sell_order_id } else { t.buy_order_id };
                        let entry = state.order_history.entry(maker_id).or_insert(OrderTrack {
                            state: TrackState::Resting,
                            original_quantity: t.quantity,
                            filled_quantity: Decimal::ZERO,
                            avg_fill_price: None,
                        });
                        entry.filled_quantity += t.quantity;
                        entry.avg_fill_price = Some(t.price);
                    }

                    // Stop-loss triggers based on the new last price.
                    if let Some(last) = state.book.last_price() {
                        let triggered = state.stops.drain_triggered(last);
                        for mut t in triggered {
                            // Convert to MARKET for execution.
                            t.order_type = OrderType::Market;
                            if let Ok(r) = state.book.match_order(t) {
                                if !r.trades.is_empty() {
                                    trades_emitted += r.trades.len() as u64;
                                    let snap = state.book.get_snapshot();
                                    let _ = publisher.publish_batch(&r.trades, Some(&snap)).await;
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "match rejected");
                metrics::rejects().with_label_values(&["MATCH_ERR"]).inc();
                state.order_history.insert(
                    order_id,
                    OrderTrack {
                        state: TrackState::Rejected,
                        original_quantity: original_qty,
                        filled_quantity: Decimal::ZERO,
                        avg_fill_price: None,
                    },
                );
            }
        }
        Ok(trades_emitted)
    }

    fn process_cancel(state: &mut SymbolState, msg: &OrderEventMsg) {
        if let Some(removed) = state.stops.remove(&msg.order_id) {
            info!(order_id = %removed.id, "stop order cancelled");
            state.order_history.entry(removed.id).and_modify(|t| t.state = TrackState::Cancelled);
            return;
        }
        match state.book.cancel_order(msg.order_id) {
            Ok(o) => {
                info!(order_id = %o.id, "order cancelled");
                state.order_history.entry(o.id)
                    .and_modify(|t| t.state = TrackState::Cancelled)
                    .or_insert(OrderTrack {
                        state: TrackState::Cancelled,
                        original_quantity: o.quantity,
                        filled_quantity: o.quantity - o.remaining,
                        avg_fill_price: None,
                    });
            }
            Err(e) => warn!(error = %e, order_id = %msg.order_id, "cancel failed"),
        }
    }

    /// Force a snapshot flush for every symbol with `force_if_idle = true` (heartbeat) or
    /// only those whose threshold/timer expired (`false`).
    pub async fn flush_due_snapshots(&mut self, force_if_idle: bool) -> anyhow::Result<()> {
        let keys: Vec<String> = self.symbols.keys().cloned().collect();
        for sym in keys {
            let due = {
                let state = match self.symbols.get(&sym) {
                    Some(s) => s,
                    None => continue,
                };
                state.events_since_snapshot >= self.event_threshold
                    || state.last_snapshot_at.elapsed() >= self.snapshot_interval
                    || force_if_idle
            };
            if due {
                self.flush_one(&sym).await?;
            }
        }
        Ok(())
    }

    async fn flush_one(&mut self, symbol: &str) -> anyhow::Result<()> {
        let snap = match self.symbols.get(symbol) {
            Some(s) => s.book.get_snapshot(),
            None => return Ok(()),
        };
        self.redis.write(&snap).await?;
        metrics::snapshots().with_label_values(&[symbol]).inc();
        if let Some(state) = self.symbols.get_mut(symbol) {
            state.events_since_snapshot = 0;
            state.last_snapshot_at = Instant::now();
        }
        Ok(())
    }
}

fn build_order(msg: &OrderEventMsg) -> anyhow::Result<Order> {
    Ok(Order::new(
        msg.order_id,
        msg.user_id,
        msg.symbol.clone(),
        msg.side.into_engine(),
        msg.order_type.into_engine(),
        msg.price,
        msg.stop_price,
        msg.quantity,
        chrono::Utc::now().timestamp_millis(),
    ))
}

#[allow(dead_code)]
fn _phantom_side(_: Side) {}

/// Outcome of a gRPC-initiated cancel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    Cancelled,
    NotFound,
    AlreadyTerminal,
}
