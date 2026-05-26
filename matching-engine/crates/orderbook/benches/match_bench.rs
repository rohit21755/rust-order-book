//! Criterion benchmarks for OrderBook hot path.
//!
//! Targets:
//! - `match_limit_taker_against_pre_seeded_book` — single fill latency
//! - `throughput_100k_random_limits`             — sustained insert throughput

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use orderbook::{Order, OrderBook, OrderType, Side};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use uuid::Uuid;

fn seed_book(book: &mut OrderBook, levels: u32, depth_per_level: u32) {
    let user = Uuid::new_v4();
    for i in 0..levels {
        let bid_price = dec!(100) - Decimal::from(i);
        let ask_price = dec!(101) + Decimal::from(i);
        for _ in 0..depth_per_level {
            let bid = Order::new(
                Uuid::new_v4(), user, "BTC-USDT", Side::Buy, OrderType::Limit,
                Some(bid_price), None, dec!(1), 0,
            );
            let ask = Order::new(
                Uuid::new_v4(), user, "BTC-USDT", Side::Sell, OrderType::Limit,
                Some(ask_price), None, dec!(1), 0,
            );
            let _ = book.add_order(bid);
            let _ = book.add_order(ask);
        }
    }
}

fn bench_single_match(c: &mut Criterion) {
    let mut group = c.benchmark_group("match");
    group.throughput(Throughput::Elements(1));

    group.bench_function("limit_taker_vs_seeded_book_100x4", |b| {
        b.iter_batched(
            || {
                let mut book = OrderBook::new("BTC-USDT");
                seed_book(&mut book, 100, 4);
                book
            },
            |mut book| {
                let taker = Order::new(
                    Uuid::new_v4(), Uuid::new_v4(), "BTC-USDT", Side::Buy, OrderType::Limit,
                    Some(dec!(101)), None, dec!(1), 0,
                );
                let _ = black_box(book.match_order(taker));
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_insert_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert");
    group.throughput(Throughput::Elements(1));

    group.bench_function("add_limit_random_price", |b| {
        let mut book = OrderBook::new("BTC-USDT");
        let user = Uuid::new_v4();
        let mut counter: u64 = 0;
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let price = dec!(100) + Decimal::from(counter % 50);
            let o = Order::new(
                Uuid::new_v4(), user, "BTC-USDT", Side::Sell, OrderType::Limit,
                Some(price), None, dec!(1), 0,
            );
            let _ = black_box(book.add_order(o));
        });
    });

    group.finish();
}

criterion_group!(benches, bench_single_match, bench_insert_throughput);
criterion_main!(benches);
