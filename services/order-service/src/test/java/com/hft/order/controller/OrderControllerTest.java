package com.hft.order.controller;

import com.hft.order.domain.OrderSide;
import com.hft.order.domain.OrderStatus;
import com.hft.order.domain.OrderType;
import com.hft.order.dto.OrderRequest;
import com.hft.order.dto.OrderResponse;
import com.hft.grpc.client.MatchingEngineGrpcClient;
import com.hft.matching.grpc.OrderBookResponse;
import com.hft.matching.grpc.PriceLevel;
import com.hft.order.exception.GlobalExceptionHandler;
import com.hft.order.service.OrderService;
import com.hft.shared.error.BusinessException;
import com.hft.shared.error.ErrorCode;
import com.hft.shared.security.AuthenticatedUserPrincipal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OrderControllerTest {

    OrderService service;
    MatchingEngineGrpcClient engineClient;
    UUID userId;
    AuthenticatedUserPrincipal principal;

    @BeforeEach
    void setUp() {
        service = mock(OrderService.class);
        engineClient = mock(MatchingEngineGrpcClient.class);
        userId = UUID.randomUUID();
        principal = new AuthenticatedUserPrincipal(userId, "t",
                List.of(new SimpleGrantedAuthority("ROLE_TRADER")));
    }

    private WebTestClient client() {
        return WebTestClient.bindToController(new OrderController(service, engineClient))
                .controllerAdvice(new GlobalExceptionHandler())
                .build()
                .mutateWith(SecurityMockServerConfigurers.mockAuthentication(principal));
    }

    private OrderResponse fakeResp(UUID id) {
        return new OrderResponse(id, userId, "BTC-USDT", "BUY", "LIMIT",
                BigDecimal.TEN, null, BigDecimal.ONE, BigDecimal.ZERO, null,
                OrderStatus.QUEUED.name(), "k", null,
                OffsetDateTime.now(), OffsetDateTime.now());
    }

    @Test
    void submitReturns201() {
        UUID id = UUID.randomUUID();
        when(service.submit(any(), any(OrderRequest.class))).thenReturn(Mono.just(fakeResp(id)));

        client().post().uri("/api/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(new OrderRequest("BTC-USDT", OrderSide.BUY, OrderType.LIMIT,
                        new BigDecimal("10"), null, new BigDecimal("1"), "k"))
                .exchange()
                .expectStatus().isCreated()
                .expectBody()
                .jsonPath("$.status").isEqualTo("QUEUED")
                .jsonPath("$.symbol").isEqualTo("BTC-USDT");
    }

    @Test
    void submitValidationFailureReturns400WithCode() {
        client().post().uri("/api/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(new OrderRequest("", null, null, null, null, null, ""))
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.code").isEqualTo(ErrorCode.VALIDATION_FAILED.name());
    }

    @Test
    void notFoundReturns404WithCode() {
        UUID id = UUID.randomUUID();
        when(service.get(id)).thenReturn(Mono.error(
                BusinessException.notFound(ErrorCode.ORDER_NOT_FOUND, "Order not found")));

        client().get().uri("/api/orders/{id}", id)
                .exchange()
                .expectStatus().isNotFound()
                .expectBody()
                .jsonPath("$.code").isEqualTo(ErrorCode.ORDER_NOT_FOUND.name());
    }

    @Test
    void cancelReturnsOk() {
        UUID id = UUID.randomUUID();
        OrderResponse r = new OrderResponse(id, userId, "BTC-USDT", "BUY", "LIMIT",
                BigDecimal.TEN, null, BigDecimal.ONE, BigDecimal.ZERO, null,
                OrderStatus.CANCELLED.name(), "k", "user requested",
                OffsetDateTime.now(), OffsetDateTime.now());
        when(service.cancel(any(), any())).thenReturn(Mono.just(r));

        client().delete().uri("/api/orders/{id}", id)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.status").isEqualTo("CANCELLED");
    }

    @Test
    void bookReturnsGrpcSnapshot() {
        OrderBookResponse resp = OrderBookResponse.newBuilder()
                .setSymbol("BTC-USDT")
                .setSequence(9)
                .addBids(PriceLevel.newBuilder().setPrice("100").setQuantity("2").setOrderCount(1))
                .addAsks(PriceLevel.newBuilder().setPrice("101").setQuantity("3").setOrderCount(1))
                .build();
        when(engineClient.getOrderBook(anyString(), anyInt())).thenReturn(Mono.just(resp));

        client().get().uri("/api/orders/book/{symbol}?depth=10", "BTC-USDT")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.symbol").isEqualTo("BTC-USDT")
                .jsonPath("$.sequence").isEqualTo(9)
                .jsonPath("$.bids[0].price").isEqualTo("100");
    }
}
