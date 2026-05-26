//! Core `OrderBook` implementation.

use crate::error::{BookError, BookResult};
use crate::order::{Order, OrderId, OrderType, Side, UserId};
use crate::price_level::PriceLevel;
use crate::snapshot::{Level, OrderBookSnapshot};
use crate::trade::Trade;
use rust_decimal::Decimal;
use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap};
use uuid::Uuid;

/// Result of submitting a single incoming order to the book.
#[derive(Debug, Clone, Default)]
pub struct MatchResult {
    /// Trades produced by this submission (may be empty).
    pub trades: Vec<Trade>,
    /// Remaining quantity placed on the book (0 if fully filled or order was MARKET).
    pub resting_quantity: Decimal,
    /// `true` if a partial fill remains resting on the book.
    pub resting: bool,
}

/// Single-symbol limit order book.
///
/// Maintains:
/// - `bids: BTreeMap<Reverse<Decimal>, PriceLevel>` — descending iteration order (best bid first)
/// - `asks: BTreeMap<Decimal, PriceLevel>`          — ascending iteration order (best ask first)
/// - `order_index: HashMap<OrderId, (Side, Decimal)>` — O(1) lookup for cancels
/// - `last_price`, `sequence` — execution state
#[derive(Debug)]
pub struct OrderBook {
    /// Symbol this book serves.
    pub symbol: String,
    bids: BTreeMap<Reverse<Decimal>, PriceLevel>,
    asks: BTreeMap<Decimal, PriceLevel>,
    /// Reverse index: order id → (side, price). Used for O(log n) cancels.
    order_index: HashMap<OrderId, (Side, Decimal)>,
    /// Monotonic sequence assigned to incoming orders + emitted trades.
    sequence: u64,
    /// Last execution price (drives stop-loss triggers).
    last_price: Option<Decimal>,
}

impl OrderBook {
    /// Construct empty book for a symbol.
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            order_index: HashMap::with_capacity(4096),
            sequence: 0,
            last_price: None,
        }
    }

    /// Highest bid currently resting.
    pub fn best_bid(&self) -> Option<Decimal> {
        self.bids.keys().next().map(|Reverse(p)| *p)
    }

    /// Lowest ask currently resting.
    pub fn best_ask(&self) -> Option<Decimal> {
        self.asks.keys().next().copied()
    }

    /// Spread = best_ask − best_bid.
    pub fn spread(&self) -> Option<Decimal> {
        match (self.best_bid(), self.best_ask()) {
            (Some(b), Some(a)) => Some(a - b),
            _ => None,
        }
    }

    /// Last execution price seen.
    pub fn last_price(&self) -> Option<Decimal> {
        self.last_price
    }

    /// Current sequence counter (next emitted sequence will be sequence+1).
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Number of resting orders.
    pub fn open_orders(&self) -> usize {
        self.order_index.len()
    }

    /// Allocate the next sequence number.
    #[inline]
    fn next_seq(&mut self) -> u64 {
        self.sequence = self.sequence.saturating_add(1);
        self.sequence
    }

    /// Add a resting order without crossing. Used when caller has already determined
    /// the order should rest (no liquidity to match against, or after partial fill).
    pub fn add_order(&mut self, mut order: Order) -> BookResult<()> {
        if order.symbol != self.symbol {
            return Err(BookError::SymbolMismatch(self.symbol.clone(), order.symbol));
        }
        if order.remaining <= Decimal::ZERO {
            return Err(BookError::InvalidOrder("non-positive quantity".into()));
        }
        let price = match order.price {
            Some(p) if p > Decimal::ZERO => p,
            _ => return Err(BookError::InvalidOrder("LIMIT requires positive price".into())),
        };
        order.sequence = self.next_seq();
        self.order_index.insert(order.id, (order.side, price));

        match order.side {
            Side::Buy => self
                .bids
                .entry(Reverse(price))
                .or_insert_with(|| PriceLevel::new(price))
                .push(order),
            Side::Sell => self
                .asks
                .entry(price)
                .or_insert_with(|| PriceLevel::new(price))
                .push(order),
        }
        Ok(())
    }

    /// Cancel a resting order by id.
    pub fn cancel_order(&mut self, order_id: OrderId) -> BookResult<Order> {
        let (side, price) = self
            .order_index
            .remove(&order_id)
            .ok_or(BookError::OrderNotFound(order_id))?;

        let removed = match side {
            Side::Buy => Self::remove_from_side(&mut self.bids, Reverse(price), order_id),
            Side::Sell => Self::remove_from_side(&mut self.asks, price, order_id),
        };

        match removed {
            Some(o) => Ok(o),
            None => Err(BookError::OrderNotFound(order_id)),
        }
    }

    fn remove_from_side<K: Ord>(
        side: &mut BTreeMap<K, PriceLevel>,
        key: K,
        order_id: OrderId,
    ) -> Option<Order> {
        let level = side.get_mut(&key)?;
        let pos = level.orders.iter().position(|o| o.id == order_id)?;
        let removed = level.orders.remove(pos)?;
        level.total_quantity -= removed.remaining;
        if level.is_empty() {
            side.remove(&key);
        }
        Some(removed)
    }

    /// Submit an incoming order. Returns generated trades and resting state.
    ///
    /// Routing:
    /// - LIMIT       → match against opposite side at price ≤ limit (buy) or ≥ limit (sell); remainder rests.
    /// - MARKET      → match against opposite side regardless of price; reject if no liquidity AND remaining > 0.
    /// - STOP_LOSS   → not handled here; caller should hold in [`StopOrderStore`].
    pub fn match_order(&mut self, mut incoming: Order) -> BookResult<MatchResult> {
        if incoming.symbol != self.symbol {
            return Err(BookError::SymbolMismatch(self.symbol.clone(), incoming.symbol));
        }
        if incoming.remaining <= Decimal::ZERO {
            return Err(BookError::InvalidOrder("non-positive quantity".into()));
        }
        match incoming.order_type {
            OrderType::Limit => {
                if !incoming.price.is_some_and(|p| p > Decimal::ZERO) {
                    return Err(BookError::InvalidOrder("LIMIT requires positive price".into()));
                }
            }
            OrderType::Market => {}
            OrderType::StopLoss => {
                return Err(BookError::InvalidOrder(
                    "STOP_LOSS orders must be routed via StopOrderStore".into(),
                ));
            }
        }

        let trades = self.cross(&mut incoming)?;

        // Rest remainder on the book for LIMIT orders only.
        let mut resting = false;
        let mut resting_quantity = Decimal::ZERO;
        if !incoming.is_filled() {
            if incoming.order_type == OrderType::Limit {
                resting_quantity = incoming.remaining;
                self.add_order(incoming)?;
                resting = true;
            } else {
                // MARKET ran out of liquidity before being fully filled.
                if trades.is_empty() {
                    return Err(BookError::NoLiquidity(incoming.id));
                }
                // Partial market fill: drop the remainder (do not rest market orders).
            }
        }

        Ok(MatchResult {
            trades,
            resting_quantity,
            resting,
        })
    }

    /// Walk the opposite side, generating trades while price + quantity allow.
    fn cross(&mut self, incoming: &mut Order) -> BookResult<Vec<Trade>> {
        let mut trades: Vec<Trade> = Vec::with_capacity(4);
        let symbol = self.symbol.clone();

        match incoming.side {
            Side::Buy => loop {
                // Find best ask without holding mut borrow across mutations.
                let (price, can_match) = match self.asks.iter().next() {
                    Some((p, _)) => {
                        let p = *p;
                        let crosses = match incoming.order_type {
                            OrderType::Market => true,
                            OrderType::Limit => incoming.price.is_some_and(|lp| p <= lp),
                            OrderType::StopLoss => false,
                        };
                        (p, crosses)
                    }
                    None => break,
                };
                if !can_match || incoming.is_filled() {
                    break;
                }
                if !self.cross_one_level(&mut trades, incoming, price, Side::Sell, &symbol)? {
                    break;
                }
            },
            Side::Sell => loop {
                let (price, can_match) = match self.bids.iter().next() {
                    Some((Reverse(p), _)) => {
                        let p = *p;
                        let crosses = match incoming.order_type {
                            OrderType::Market => true,
                            OrderType::Limit => incoming.price.is_some_and(|lp| p >= lp),
                            OrderType::StopLoss => false,
                        };
                        (p, crosses)
                    }
                    None => break,
                };
                if !can_match || incoming.is_filled() {
                    break;
                }
                if !self.cross_one_level(&mut trades, incoming, price, Side::Buy, &symbol)? {
                    break;
                }
            },
        }

        if let Some(last) = trades.last() {
            self.last_price = Some(last.price);
        }
        Ok(trades)
    }

    /// Cross against the head of `opposite_side` at `price`. Returns `true` if work was done
    /// (so the outer loop should re-check), `false` if the level was exhausted without trades
    /// (which currently never happens, but kept defensive).
    fn cross_one_level(
        &mut self,
        trades: &mut Vec<Trade>,
        incoming: &mut Order,
        price: Decimal,
        opposite_side: Side,
        symbol: &str,
    ) -> BookResult<bool> {
        // Pop the maker FIFO-style and possibly the entire level.
        let (maker, level_emptied) = match opposite_side {
            Side::Sell => Self::pop_maker(&mut self.asks, price)?,
            Side::Buy => Self::pop_maker(&mut self.bids, Reverse(price))?,
        };

        // Self-trade prevention.
        if maker.user_id == incoming.user_id {
            // Put maker back at the FRONT of its level (preserve time priority).
            Self::reinsert_front(
                match opposite_side {
                    Side::Sell => MapRef::Asks(&mut self.asks),
                    Side::Buy => MapRef::Bids(&mut self.bids),
                },
                price,
                maker,
            );
            return Err(BookError::SelfTradePrevented(incoming.user_id));
        }

        let qty = incoming.remaining.min(maker.remaining);
        incoming.remaining -= qty;

        let mut maker_after = maker;
        maker_after.remaining -= qty;

        let trade_sequence = self.next_seq();
        let (buy_order_id, sell_order_id, buyer, seller) = match incoming.side {
            Side::Buy => (
                incoming.id,
                maker_after.id,
                incoming.user_id,
                maker_after.user_id,
            ),
            Side::Sell => (
                maker_after.id,
                incoming.id,
                maker_after.user_id,
                incoming.user_id,
            ),
        };

        trades.push(Trade {
            trade_id: Uuid::new_v4(),
            buy_order_id,
            sell_order_id,
            buyer_user_id: buyer,
            seller_user_id: seller,
            symbol: symbol.to_string(),
            price,
            quantity: qty,
            executed_at_ms: now_ms(),
            sequence: trade_sequence,
        });

        // Decide what to do with the maker remainder.
        if maker_after.remaining > Decimal::ZERO {
            // Put back at the FRONT (it was the oldest and still has time priority).
            Self::reinsert_front(
                match opposite_side {
                    Side::Sell => MapRef::Asks(&mut self.asks),
                    Side::Buy => MapRef::Bids(&mut self.bids),
                },
                price,
                maker_after,
            );
        } else {
            // Maker fully consumed — drop from index.
            self.order_index.remove(&maker_after.id);
            // The level may already be empty (handled in `pop_maker`); if not, ensure cleanup.
            if level_emptied {
                // nothing to do — `pop_maker` already removed the level.
            }
        }
        Ok(true)
    }

    /// Pop the FIFO-head order from `(map, key)`. If the level becomes empty the entry is removed.
    fn pop_maker<K: Ord + Clone>(
        map: &mut BTreeMap<K, PriceLevel>,
        key: K,
    ) -> BookResult<(Order, bool)> {
        let level = map
            .get_mut(&key)
            .ok_or_else(|| BookError::InvalidOrder("expected level".into()))?;
        let order = level
            .orders
            .pop_front()
            .ok_or_else(|| BookError::InvalidOrder("expected order".into()))?;
        level.total_quantity -= order.remaining;
        let emptied = level.is_empty();
        if emptied {
            map.remove(&key);
        }
        Ok((order, emptied))
    }

    fn reinsert_front(map: MapRef<'_>, price: Decimal, order: Order) {
        match map {
            MapRef::Asks(asks) => {
                let level = asks.entry(price).or_insert_with(|| PriceLevel::new(price));
                level.total_quantity += order.remaining;
                level.orders.push_front(order);
            }
            MapRef::Bids(bids) => {
                let level = bids
                    .entry(Reverse(price))
                    .or_insert_with(|| PriceLevel::new(price));
                level.total_quantity += order.remaining;
                level.orders.push_front(order);
            }
        }
    }

    /// Per-level order counts on the bid side, top `depth` levels (0 → [`crate::SNAPSHOT_DEPTH`]).
    pub fn bid_counts(&self, depth: usize) -> Vec<u32> {
        let take = if depth == 0 { crate::SNAPSHOT_DEPTH } else { depth };
        self.bids.values().take(take).map(|l| l.orders.len() as u32).collect()
    }

    /// Per-level order counts on the ask side, top `depth` levels (0 → [`crate::SNAPSHOT_DEPTH`]).
    pub fn ask_counts(&self, depth: usize) -> Vec<u32> {
        let take = if depth == 0 { crate::SNAPSHOT_DEPTH } else { depth };
        self.asks.values().take(take).map(|l| l.orders.len() as u32).collect()
    }

    /// Top-N snapshot for persistence / market data.
    pub fn get_snapshot(&self) -> OrderBookSnapshot {
        let depth = crate::SNAPSHOT_DEPTH;
        let bids: Vec<Level> = self
            .bids
            .iter()
            .take(depth)
            .map(|(Reverse(p), lvl)| Level {
                price: *p,
                quantity: lvl.total_quantity,
            })
            .collect();
        let asks: Vec<Level> = self
            .asks
            .iter()
            .take(depth)
            .map(|(p, lvl)| Level {
                price: *p,
                quantity: lvl.total_quantity,
            })
            .collect();
        OrderBookSnapshot {
            symbol: self.symbol.clone(),
            sequence: self.sequence,
            bids,
            asks,
            timestamp_ms: now_ms(),
        }
    }

    /// Replace book state from a snapshot. Used on engine restart.
    ///
    /// This restores aggregate liquidity only — individual order ids are lost because
    /// snapshots aggregate. Subsequent orders (from Kafka replay) will populate the
    /// full index again.
    pub fn restore_from_snapshot(&mut self, snap: &OrderBookSnapshot) {
        self.bids.clear();
        self.asks.clear();
        self.order_index.clear();
        self.sequence = snap.sequence;

        for level in &snap.bids {
            let synthetic = synthetic_order_for_restore(&self.symbol, Side::Buy, level.price, level.quantity);
            let mut pl = PriceLevel::new(level.price);
            pl.total_quantity = level.quantity;
            pl.orders.push_back(synthetic);
            self.bids.insert(Reverse(level.price), pl);
        }
        for level in &snap.asks {
            let synthetic = synthetic_order_for_restore(&self.symbol, Side::Sell, level.price, level.quantity);
            let mut pl = PriceLevel::new(level.price);
            pl.total_quantity = level.quantity;
            pl.orders.push_back(synthetic);
            self.asks.insert(level.price, pl);
        }
    }

    /// Allow callers to set last_price from external state (e.g. on restore).
    pub fn set_last_price(&mut self, price: Decimal) {
        self.last_price = Some(price);
    }

    /// Reserved user id used for synthetic orders produced by snapshot restore.
    pub const RESTORE_USER_ID: Uuid = Uuid::nil();
}

/// Helper enum to avoid two near-identical methods reinserting into bids vs asks.
enum MapRef<'a> {
    Asks(&'a mut BTreeMap<Decimal, PriceLevel>),
    Bids(&'a mut BTreeMap<Reverse<Decimal>, PriceLevel>),
}

fn synthetic_order_for_restore(symbol: &str, side: Side, price: Decimal, qty: Decimal) -> Order {
    Order {
        id: Uuid::new_v4(),
        user_id: OrderBook::RESTORE_USER_ID,
        symbol: symbol.to_string(),
        side,
        order_type: OrderType::Limit,
        price: Some(price),
        stop_price: None,
        quantity: qty,
        remaining: qty,
        sequence: 0,
        timestamp_ms: now_ms(),
    }
}

#[inline]
fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Convenience: extract the unused `_user_id` reference to keep the API ergonomic.
#[allow(dead_code)]
fn _unused_user_id_marker(_: UserId) {}
