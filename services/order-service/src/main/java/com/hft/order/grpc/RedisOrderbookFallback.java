package com.hft.order.grpc;

import com.hft.grpc.fallback.OrderbookFallback;
import com.hft.matching.grpc.OrderBookResponse;
import com.hft.matching.grpc.PriceLevel;
import com.hft.order.redis.OrderbookSnapshotReader;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/**
 * Fallback for the gRPC client: reads the Redis orderbook snapshot written by the engine
 * and maps it to the gRPC {@link OrderBookResponse} shape. Used when the engine gRPC call
 * fails or the circuit breaker is open.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RedisOrderbookFallback implements OrderbookFallback {

    private final OrderbookSnapshotReader reader;

    @Override
    public Mono<OrderBookResponse> getOrderBook(String symbol, int depth) {
        return reader.snapshot(symbol)
                .map(snap -> {
                    OrderBookResponse.Builder b = OrderBookResponse.newBuilder()
                            .setSymbol(snap.symbol())
                            .setSequence(snap.sequence())
                            .setTimestampMs(System.currentTimeMillis());
                    int limit = depth <= 0 ? Integer.MAX_VALUE : depth;
                    snap.bids().stream().limit(limit).forEach(l -> b.addBids(level(l)));
                    snap.asks().stream().limit(limit).forEach(l -> b.addAsks(level(l)));
                    return b.build();
                })
                .doOnSubscribe(s -> log.debug("serving orderbook {} from Redis fallback", symbol));
    }

    private PriceLevel level(com.hft.order.dto.OrderbookSnapshot.Level l) {
        return PriceLevel.newBuilder()
                .setPrice(l.price().toPlainString())
                .setQuantity(l.quantity().toPlainString())
                .setOrderCount(0) // snapshot aggregates; per-level count unknown
                .build();
    }
}
