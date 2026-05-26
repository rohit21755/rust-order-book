//! Stop-loss order storage. Triggered when the last trade price crosses the stop price.

use crate::order::{Order, OrderId, Side};
use rust_decimal::Decimal;
use std::collections::HashMap;

/// Stores stop-loss orders pending trigger.
///
/// Trigger rule:
/// - BUY stop  → triggers when `last_price >= stop_price` (e.g. stop-buy above market)
/// - SELL stop → triggers when `last_price <= stop_price` (stop-sell below market)
#[derive(Debug, Default)]
pub struct StopOrderStore {
    by_id: HashMap<OrderId, Order>,
}

impl StopOrderStore {
    /// New empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a stop order.
    pub fn insert(&mut self, order: Order) {
        self.by_id.insert(order.id, order);
    }

    /// Remove by id; returns the removed order if present.
    pub fn remove(&mut self, id: &OrderId) -> Option<Order> {
        self.by_id.remove(id)
    }

    /// Number of resting stop orders.
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    /// True if empty.
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    /// Drain all orders triggered by `last_price`.
    ///
    /// Returns the triggered orders, removing them from the store. Caller should
    /// then re-submit them as MARKET orders (or LIMIT at stop_price, per policy).
    pub fn drain_triggered(&mut self, last_price: Decimal) -> Vec<Order> {
        let triggered: Vec<OrderId> = self
            .by_id
            .iter()
            .filter(|(_, o)| {
                let stop = match o.stop_price {
                    Some(p) => p,
                    None => return false,
                };
                match o.side {
                    Side::Buy => last_price >= stop,
                    Side::Sell => last_price <= stop,
                }
            })
            .map(|(id, _)| *id)
            .collect();

        triggered
            .into_iter()
            .filter_map(|id| self.by_id.remove(&id))
            .collect()
    }
}
