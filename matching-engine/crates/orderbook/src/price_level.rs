//! FIFO queue of resting orders at a single price level.

use crate::order::Order;
use rust_decimal::Decimal;
use std::collections::VecDeque;

/// Price level: a price + FIFO-ordered resting orders.
///
/// `total_quantity` is maintained incrementally so `OrderBook::get_snapshot`
/// is O(depth) instead of O(orders).
#[derive(Debug, Clone)]
pub struct PriceLevel {
    /// Limit price.
    pub price: Decimal,
    /// Orders at this price, oldest first.
    pub orders: VecDeque<Order>,
    /// Sum of remaining quantities across all orders.
    pub total_quantity: Decimal,
}

impl PriceLevel {
    /// Create an empty level.
    pub fn new(price: Decimal) -> Self {
        Self {
            price,
            orders: VecDeque::with_capacity(16),
            total_quantity: Decimal::ZERO,
        }
    }

    /// Append a new order. Quantities are summed into `total_quantity`.
    #[inline]
    pub fn push(&mut self, order: Order) {
        self.total_quantity += order.remaining;
        self.orders.push_back(order);
    }

    /// `true` if no orders remain at this level.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }
}
