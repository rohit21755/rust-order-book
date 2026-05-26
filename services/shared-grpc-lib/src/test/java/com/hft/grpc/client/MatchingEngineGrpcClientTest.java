package com.hft.grpc.client;

import com.hft.grpc.config.GrpcChannelPool;
import com.hft.grpc.config.GrpcClientProperties;
import com.hft.grpc.fallback.OrderbookFallback;
import com.hft.matching.grpc.HealthResponse;
import com.hft.matching.grpc.HealthStatus;
import com.hft.matching.grpc.MatchingEngineServiceGrpc;
import com.hft.matching.grpc.OrderBookRequest;
import com.hft.matching.grpc.OrderBookResponse;
import com.hft.matching.grpc.PriceLevel;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * In-process gRPC server tests. No real network — uses io.grpc.inprocess.
 * We subclass GrpcChannelPool to feed it the in-process channel.
 */
class MatchingEngineGrpcClientTest {

    private Server server;
    private ManagedChannel channel;
    private String serverName;

    @BeforeEach
    void setUp() {
        serverName = InProcessServerBuilder.generateName();
    }

    @AfterEach
    void tearDown() {
        if (channel != null) channel.shutdownNow();
        if (server != null) server.shutdownNow();
    }

    private void startServer(MatchingEngineServiceGrpc.MatchingEngineServiceImplBase svc) throws Exception {
        server = InProcessServerBuilder.forName(serverName)
                .directExecutor().addService(svc).build().start();
        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
    }

    private MatchingEngineGrpcClient clientWith(ManagedChannel ch, OrderbookFallback fallback) {
        GrpcClientProperties props = new GrpcClientProperties();
        props.setDeadlineMs(2000);
        // Pool that always returns the in-process channel.
        GrpcChannelPool pool = new GrpcChannelPool(props) {
            @Override
            public ManagedChannel next() {
                return ch;
            }
        };
        CircuitBreaker cb = CircuitBreaker.ofDefaults("test");
        return new MatchingEngineGrpcClient(pool, props, cb, fallback);
    }

    @Test
    void getOrderBookReturnsServerResponse() throws Exception {
        startServer(new MatchingEngineServiceGrpc.MatchingEngineServiceImplBase() {
            @Override
            public void getOrderBook(OrderBookRequest request, StreamObserver<OrderBookResponse> obs) {
                obs.onNext(OrderBookResponse.newBuilder()
                        .setSymbol(request.getSymbol())
                        .setSequence(7)
                        .addBids(PriceLevel.newBuilder().setPrice("100").setQuantity("2").setOrderCount(1))
                        .addAsks(PriceLevel.newBuilder().setPrice("101").setQuantity("3").setOrderCount(2))
                        .build());
                obs.onCompleted();
            }
        });

        var client = clientWith(channel, (s, d) -> Mono.error(new IllegalStateException("should not fall back")));

        StepVerifier.create(client.getOrderBook("BTC-USDT", 10))
                .assertNext(resp -> {
                    org.assertj.core.api.Assertions.assertThat(resp.getSymbol()).isEqualTo("BTC-USDT");
                    org.assertj.core.api.Assertions.assertThat(resp.getSequence()).isEqualTo(7);
                    org.assertj.core.api.Assertions.assertThat(resp.getBidsCount()).isEqualTo(1);
                    org.assertj.core.api.Assertions.assertThat(resp.getBids(0).getPrice()).isEqualTo("100");
                })
                .verifyComplete();
    }

    @Test
    void getOrderBookFallsBackOnServerError() throws Exception {
        startServer(new MatchingEngineServiceGrpc.MatchingEngineServiceImplBase() {
            @Override
            public void getOrderBook(OrderBookRequest request, StreamObserver<OrderBookResponse> obs) {
                obs.onError(io.grpc.Status.UNAVAILABLE.withDescription("engine down").asRuntimeException());
            }
        });

        AtomicBoolean fellBack = new AtomicBoolean(false);
        OrderbookFallback fallback = (s, d) -> {
            fellBack.set(true);
            return Mono.just(OrderBookResponse.newBuilder().setSymbol(s).setSequence(0).build());
        };
        var client = clientWith(channel, fallback);

        StepVerifier.create(client.getOrderBook("ETH-USDT", 5))
                .assertNext(resp -> org.assertj.core.api.Assertions.assertThat(resp.getSymbol()).isEqualTo("ETH-USDT"))
                .verifyComplete();
        org.assertj.core.api.Assertions.assertThat(fellBack).isTrue();
    }

    @Test
    void healthReturnsServing() throws Exception {
        startServer(new MatchingEngineServiceGrpc.MatchingEngineServiceImplBase() {
            @Override
            public void getEngineHealth(com.hft.matching.grpc.Empty request, StreamObserver<HealthResponse> obs) {
                obs.onNext(HealthResponse.newBuilder()
                        .setStatus(HealthStatus.SERVING)
                        .setOrdersProcessed(42)
                        .setTradesExecuted(7)
                        .setUptimeMs(1000)
                        .setVersion("0.1.0")
                        .build());
                obs.onCompleted();
            }
        });

        var client = clientWith(channel, (s, d) -> Mono.empty());
        StepVerifier.create(client.getEngineHealth())
                .assertNext(h -> {
                    org.assertj.core.api.Assertions.assertThat(h.getStatus()).isEqualTo(HealthStatus.SERVING);
                    org.assertj.core.api.Assertions.assertThat(h.getOrdersProcessed()).isEqualTo(42);
                })
                .verifyComplete();
    }
}
