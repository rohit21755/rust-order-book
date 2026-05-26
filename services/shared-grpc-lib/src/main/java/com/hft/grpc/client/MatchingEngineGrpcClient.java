package com.hft.grpc.client;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.hft.grpc.config.GrpcChannelPool;
import com.hft.grpc.config.GrpcClientProperties;
import com.hft.grpc.fallback.OrderbookFallback;
import com.hft.matching.grpc.CancelOrderRequest;
import com.hft.matching.grpc.CancelOrderResponse;
import com.hft.matching.grpc.Empty;
import com.hft.matching.grpc.HealthResponse;
import com.hft.matching.grpc.MatchingEngineServiceGrpc;
import com.hft.matching.grpc.MatchingEngineServiceGrpc.MatchingEngineServiceFutureStub;
import com.hft.matching.grpc.OrderBookRequest;
import com.hft.matching.grpc.OrderBookResponse;
import com.hft.matching.grpc.OrderStatusRequest;
import com.hft.matching.grpc.OrderStatusResponse;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import io.grpc.Deadline;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeUnit;

/**
 * Reactive gRPC client for the matching engine.
 *
 * - Each call uses a per-call deadline + the round-robin channel pool.
 * - Wrapped in a Resilience4j circuit breaker.
 * - {@link #getOrderBook} falls back to {@link OrderbookFallback} (Redis) on failure / open circuit.
 */
@Slf4j
public class MatchingEngineGrpcClient {

    private final GrpcChannelPool pool;
    private final GrpcClientProperties props;
    private final CircuitBreaker circuitBreaker;
    private final OrderbookFallback fallback;

    public MatchingEngineGrpcClient(
            GrpcChannelPool pool,
            GrpcClientProperties props,
            CircuitBreaker circuitBreaker,
            OrderbookFallback fallback) {
        this.pool = pool;
        this.props = props;
        this.circuitBreaker = circuitBreaker;
        this.fallback = fallback;
    }

    private MatchingEngineServiceFutureStub stub() {
        return MatchingEngineServiceGrpc.newFutureStub(pool.next())
                .withDeadline(Deadline.after(props.getDeadlineMs(), TimeUnit.MILLISECONDS));
    }

    /** Orderbook query: gRPC primary → circuit breaker → Redis fallback. */
    public Mono<OrderBookResponse> getOrderBook(String symbol, int depth) {
        OrderBookRequest req = OrderBookRequest.newBuilder()
                .setSymbol(symbol)
                .setDepth(depth)
                .build();

        return toMono(() -> stub().getOrderBook(req))
                .transformDeferred(CircuitBreakerOperator.of(circuitBreaker))
                .onErrorResume(err -> {
                    log.warn("gRPC getOrderBook failed for {} ({}); falling back to Redis snapshot",
                            symbol, err.toString());
                    return fallback.getOrderBook(symbol, depth);
                });
    }

    /** Engine health (no fallback). */
    public Mono<HealthResponse> getEngineHealth() {
        return toMono(() -> stub().getEngineHealth(Empty.getDefaultInstance()))
                .transformDeferred(CircuitBreakerOperator.of(circuitBreaker));
    }

    /** Order status (no fallback). */
    public Mono<OrderStatusResponse> getOrderStatus(String symbol, String orderId) {
        OrderStatusRequest req = OrderStatusRequest.newBuilder()
                .setSymbol(symbol)
                .setOrderId(orderId)
                .build();
        return toMono(() -> stub().getOrderStatus(req))
                .transformDeferred(CircuitBreakerOperator.of(circuitBreaker));
    }

    /** Direct cancel (bypasses Kafka). No fallback — caller decides on failure. */
    public Mono<CancelOrderResponse> cancelOrder(String symbol, String orderId, String userId) {
        CancelOrderRequest req = CancelOrderRequest.newBuilder()
                .setSymbol(symbol)
                .setOrderId(orderId)
                .setUserId(userId)
                .build();
        return toMono(() -> stub().cancelOrder(req))
                .transformDeferred(CircuitBreakerOperator.of(circuitBreaker));
    }

    /** Bridge a gRPC ListenableFuture into a Reactor Mono (non-blocking). */
    private static <T> Mono<T> toMono(java.util.function.Supplier<ListenableFuture<T>> call) {
        return Mono.create(sink -> {
            ListenableFuture<T> future;
            try {
                future = call.get();
            } catch (Exception e) {
                sink.error(e);
                return;
            }
            Futures.addCallback(future, new FutureCallback<>() {
                @Override
                public void onSuccess(T result) {
                    sink.success(result);
                }

                @Override
                public void onFailure(Throwable t) {
                    sink.error(t);
                }
            }, MoreExecutors.directExecutor());
            sink.onCancel(() -> future.cancel(true));
        });
    }
}
