package com.hft.grpc.fallback;

import com.hft.matching.grpc.OrderBookResponse;
import reactor.core.publisher.Mono;

/**
 * Fallback source used when the matching engine gRPC call fails or the circuit is open.
 * Services provide an implementation (typically reading the Redis orderbook snapshot).
 *
 * Default bean is a no-op that errors; override by declaring your own {@code @Bean}.
 */
@FunctionalInterface
public interface OrderbookFallback {
    Mono<OrderBookResponse> getOrderBook(String symbol, int depth);
}
