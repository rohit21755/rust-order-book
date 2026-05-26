//! Integration tests for OrderBook matching logic.

use orderbook::{
    BookError, Order, OrderBook, OrderType, Side, StopOrderStore, OrderId,
};
use rust_decimal_macros::dec;
use uuid::Uuid;

fn mk_order(
    user: Uuid,
    side: Side,
    ty: OrderType,
    price: Option<rust_decimal::Decimal>,
    qty: rust_decimal::Decimal,
) -> Order {
    Order::new(
        Uuid::new_v4(),
        user,
        "BTC-USDT",
        side,
        ty,
        price,
        None,
        qty,
        0,
    )
}

#[test]
fn add_resting_limit_orders_and_check_bbo() {
    let mut book = OrderBook::new("BTC-USDT");
    let alice = Uuid::new_v4();
    book.add_order(mk_order(alice, Side::Buy, OrderType::Limit, Some(dec!(100)), dec!(1)))
        .unwrap();
    book.add_order(mk_order(alice, Side::Buy, OrderType::Limit, Some(dec!(99)), dec!(2)))
        .unwrap();
    book.add_order(mk_order(alice, Side::Sell, OrderType::Limit, Some(dec!(101)), dec!(1)))
        .unwrap();

    assert_eq!(book.best_bid(), Some(dec!(100)));
    assert_eq!(book.best_ask(), Some(dec!(101)));
    assert_eq!(book.spread(), Some(dec!(1)));
}

#[test]
fn limit_buy_crosses_resting_sell_fully_fills() {
    let mut book = OrderBook::new("BTC-USDT");
    let alice = Uuid::new_v4();
    let bob = Uuid::new_v4();
    book.add_order(mk_order(alice, Side::Sell, OrderType::Limit, Some(dec!(100)), dec!(1)))
        .unwrap();

    let res = book
        .match_order(mk_order(bob, Side::Buy, OrderType::Limit, Some(dec!(100)), dec!(1)))
        .unwrap();

    assert_eq!(res.trades.len(), 1);
    assert_eq!(res.trades[0].price, dec!(100));
    assert_eq!(res.trades[0].quantity, dec!(1));
    assert!(!res.resting);
    assert_eq!(book.open_orders(), 0);
}

#[test]
fn limit_partial_fill_rests_remainder() {
    let mut book = OrderBook::new("BTC-USDT");
    let maker = Uuid::new_v4();
    let taker = Uuid::new_v4();
    book.add_order(mk_order(maker, Side::Sell, OrderType::Limit, Some(dec!(100)), dec!(1)))
        .unwrap();

    let res = book
        .match_order(mk_order(taker, Side::Buy, OrderType::Limit, Some(dec!(100)), dec!(3)))
        .unwrap();
    assert_eq!(res.trades.len(), 1);
    assert_eq!(res.trades[0].quantity, dec!(1));
    assert!(res.resting);
    assert_eq!(res.resting_quantity, dec!(2));
    assert_eq!(book.best_bid(), Some(dec!(100)));
}

#[test]
fn price_time_priority_fifo() {
    let mut book = OrderBook::new("BTC-USDT");
    let m1 = Uuid::new_v4();
    let m2 = Uuid::new_v4();
    let taker = Uuid::new_v4();
    book.add_order(mk_order(m1, Side::Sell, OrderType::Limit, Some(dec!(100)), dec!(1)))
        .unwrap();
    book.add_order(mk_order(m2, Side::Sell, OrderType::Limit, Some(dec!(100)), dec!(1)))
        .unwrap();

    let res = book
        .match_order(mk_order(taker, Side::Buy, OrderType::Limit, Some(dec!(100)), dec!(1)))
        .unwrap();
    assert_eq!(res.trades.len(), 1);
    // First maker should be filled first.
    assert_eq!(res.trades[0].seller_user_id, m1);
}

#[test]
fn market_order_with_no_liquidity_errors() {
    let mut book = OrderBook::new("BTC-USDT");
    let taker = Uuid::new_v4();
    let res = book.match_order(mk_order(taker, Side::Buy, OrderType::Market, None, dec!(1)));
    assert!(matches!(res, Err(BookError::NoLiquidity(_))));
}

#[test]
fn market_buy_consumes_multiple_levels() {
    let mut book = OrderBook::new("BTC-USDT");
    let m1 = Uuid::new_v4();
    let m2 = Uuid::new_v4();
    let taker = Uuid::new_v4();
    book.add_order(mk_order(m1, Side::Sell, OrderType::Limit, Some(dec!(100)), dec!(1)))
        .unwrap();
    book.add_order(mk_order(m2, Side::Sell, OrderType::Limit, Some(dec!(101)), dec!(1)))
        .unwrap();

    let res = book
        .match_order(mk_order(taker, Side::Buy, OrderType::Market, None, dec!(2)))
        .unwrap();
    assert_eq!(res.trades.len(), 2);
    assert_eq!(res.trades[0].price, dec!(100));
    assert_eq!(res.trades[1].price, dec!(101));
}

#[test]
fn self_trade_prevention() {
    let mut book = OrderBook::new("BTC-USDT");
    let me = Uuid::new_v4();
    book.add_order(mk_order(me, Side::Sell, OrderType::Limit, Some(dec!(100)), dec!(1)))
        .unwrap();

    let res = book.match_order(mk_order(me, Side::Buy, OrderType::Limit, Some(dec!(100)), dec!(1)));
    assert!(matches!(res, Err(BookError::SelfTradePrevented(_))));
    // Resting maker must still be present.
    assert_eq!(book.best_ask(), Some(dec!(100)));
}

#[test]
fn cancel_resting_order_removes_it() {
    let mut book = OrderBook::new("BTC-USDT");
    let alice = Uuid::new_v4();
    let mut order = mk_order(alice, Side::Buy, OrderType::Limit, Some(dec!(100)), dec!(1));
    let id: OrderId = order.id;
    order.sequence = 0;
    book.add_order(order).unwrap();
    assert_eq!(book.best_bid(), Some(dec!(100)));
    book.cancel_order(id).unwrap();
    assert!(book.best_bid().is_none());
}

#[test]
fn cancel_missing_order_errors() {
    let mut book = OrderBook::new("BTC-USDT");
    let res = book.cancel_order(Uuid::new_v4());
    assert!(matches!(res, Err(BookError::OrderNotFound(_))));
}

#[test]
fn invalid_limit_without_price_rejected() {
    let mut book = OrderBook::new("BTC-USDT");
    let alice = Uuid::new_v4();
    let bad = mk_order(alice, Side::Buy, OrderType::Limit, None, dec!(1));
    assert!(book.match_order(bad).is_err());
}

#[test]
fn stop_loss_triggers_on_price_cross() {
    let mut store = StopOrderStore::new();
    let alice = Uuid::new_v4();
    let mut stop = mk_order(alice, Side::Sell, OrderType::StopLoss, None, dec!(1));
    stop.stop_price = Some(dec!(95));
    store.insert(stop);

    // Last price 100 → not triggered.
    assert!(store.drain_triggered(dec!(100)).is_empty());
    assert_eq!(store.len(), 1);

    // Last price 94 → trigger sell-stop.
    let triggered = store.drain_triggered(dec!(94));
    assert_eq!(triggered.len(), 1);
    assert!(store.is_empty());
}

#[test]
fn snapshot_aggregates_levels() {
    let mut book = OrderBook::new("BTC-USDT");
    let alice = Uuid::new_v4();
    let bob = Uuid::new_v4();
    book.add_order(mk_order(alice, Side::Buy, OrderType::Limit, Some(dec!(100)), dec!(1)))
        .unwrap();
    book.add_order(mk_order(bob, Side::Buy, OrderType::Limit, Some(dec!(100)), dec!(2)))
        .unwrap();
    let snap = book.get_snapshot();
    assert_eq!(snap.bids.len(), 1);
    assert_eq!(snap.bids[0].price, dec!(100));
    assert_eq!(snap.bids[0].quantity, dec!(3));
}

#[test]
fn restore_from_snapshot_rebuilds_levels() {
    let mut book = OrderBook::new("BTC-USDT");
    let alice = Uuid::new_v4();
    book.add_order(mk_order(alice, Side::Buy, OrderType::Limit, Some(dec!(100)), dec!(1)))
        .unwrap();
    let snap = book.get_snapshot();

    let mut restored = OrderBook::new("BTC-USDT");
    restored.restore_from_snapshot(&snap);
    assert_eq!(restored.best_bid(), Some(dec!(100)));
    assert_eq!(restored.sequence(), snap.sequence);
}
