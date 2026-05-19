package com.hft.order.service;

import com.hft.order.domain.Order;
import com.hft.order.domain.OrderSide;
import com.hft.order.domain.OrderStatus;
import com.hft.order.domain.OrderType;
import com.hft.order.dto.OrderRequest;
import com.hft.order.kafka.OrderEvent;
import com.hft.order.kafka.OrderEventPublisher;
import com.hft.order.redis.IdempotencyService;
import com.hft.order.redis.OrderCache;
import com.hft.order.repository.OrderRepository;
import com.hft.order.validation.OrderValidator;
import com.hft.shared.error.BusinessException;
import com.hft.shared.error.ErrorCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OrderServiceTest {

    OrderRepository repo;
    OrderValidator validator;
    OrderEventPublisher publisher;
    IdempotencyService idempotency;
    OrderCache cache;
    OrderService svc;

    @BeforeEach
    void setUp() {
        repo = mock(OrderRepository.class);
        validator = mock(OrderValidator.class);
        publisher = mock(OrderEventPublisher.class);
        idempotency = mock(IdempotencyService.class);
        cache = mock(OrderCache.class);
        svc = new OrderService(repo, validator, publisher, idempotency, cache);
    }

    private OrderRequest req() {
        return new OrderRequest("BTC-USDT", OrderSide.BUY, OrderType.LIMIT,
                new BigDecimal("10"), null, new BigDecimal("1"), "idem-1");
    }

    @Test
    void submitHappyPath() {
        UUID uid = UUID.randomUUID();
        doNothing().when(validator).validateStructure(any());
        when(idempotency.reserve(any(), anyString(), any())).thenReturn(Mono.just(true));
        when(validator.validateOpenOrderLimit(any(), anyString())).thenReturn(Mono.empty());
        when(repo.save(any(Order.class))).thenAnswer(inv -> {
            Order o = inv.getArgument(0);
            o.setCreatedAt(OffsetDateTime.now());
            o.setUpdatedAt(OffsetDateTime.now());
            return Mono.just(o);
        });
        when(repo.updateStatus(any(), anyString(), any())).thenReturn(Mono.just(1));
        when(publisher.publish(any(OrderEvent.class))).thenReturn(Mono.empty());
        when(cache.put(any())).thenReturn(Mono.just(true));

        StepVerifier.create(svc.submit(uid, req()))
                .assertNext(resp -> {
                    assert resp.status().equals(OrderStatus.QUEUED.name());
                    assert resp.symbol().equals("BTC-USDT");
                    assert resp.userId().equals(uid);
                })
                .verifyComplete();
    }

    @Test
    void submitDuplicateIdemReturnsExisting() {
        UUID uid = UUID.randomUUID();
        UUID existingId = UUID.randomUUID();
        doNothing().when(validator).validateStructure(any());
        when(idempotency.reserve(any(), anyString(), any())).thenReturn(Mono.just(false));
        when(idempotency.lookup(any(), anyString())).thenReturn(Mono.just(existingId.toString()));
        Order existing = Order.builder()
                .id(existingId).userId(uid).symbol("BTC-USDT").side("BUY").type("LIMIT")
                .quantity(BigDecimal.ONE).status(OrderStatus.QUEUED.name())
                .idempotencyKey("idem-1").build();
        when(repo.findById(existingId)).thenReturn(Mono.just(existing));

        StepVerifier.create(svc.submit(uid, req()))
                .assertNext(resp -> {
                    assert resp.id().equals(existingId);
                })
                .verifyComplete();
    }

    @Test
    void publishFailureMarksSystemError() {
        UUID uid = UUID.randomUUID();
        doNothing().when(validator).validateStructure(any());
        when(idempotency.reserve(any(), anyString(), any())).thenReturn(Mono.just(true));
        when(validator.validateOpenOrderLimit(any(), anyString())).thenReturn(Mono.empty());
        when(repo.save(any(Order.class))).thenAnswer(inv -> Mono.just(inv.getArgument(0, Order.class)));
        when(repo.updateStatus(any(), anyString(), any())).thenReturn(Mono.just(1));
        when(publisher.publish(any(OrderEvent.class))).thenReturn(Mono.error(
                BusinessException.internal(ErrorCode.KAFKA_PUBLISH_FAILED, "boom")));
        when(cache.put(any())).thenReturn(Mono.just(true));

        StepVerifier.create(svc.submit(uid, req()))
                .expectErrorMatches(t -> t instanceof BusinessException be
                        && be.getCode() == ErrorCode.KAFKA_PUBLISH_FAILED)
                .verify();
    }

    @Test
    void cancelHappyPath() {
        UUID uid = UUID.randomUUID();
        UUID oid = UUID.randomUUID();
        Order o = Order.builder().id(oid).userId(uid).symbol("BTC-USDT")
                .side("BUY").type("LIMIT").quantity(BigDecimal.ONE)
                .status(OrderStatus.QUEUED.name()).idempotencyKey("k").build();
        when(repo.findById(oid)).thenReturn(Mono.just(o));
        when(repo.updateStatus(any(), anyString(), any())).thenReturn(Mono.just(1));
        when(publisher.publish(any(OrderEvent.class))).thenReturn(Mono.empty());
        when(cache.put(any())).thenReturn(Mono.just(true));

        StepVerifier.create(svc.cancel(uid, oid))
                .assertNext(resp -> {
                    assert resp.status().equals(OrderStatus.CANCELLED.name());
                })
                .verifyComplete();
    }

    @Test
    void cancelByDifferentUserForbidden() {
        UUID owner = UUID.randomUUID();
        UUID other = UUID.randomUUID();
        UUID oid = UUID.randomUUID();
        Order o = Order.builder().id(oid).userId(owner).symbol("BTC-USDT")
                .side("BUY").type("LIMIT").quantity(BigDecimal.ONE)
                .status(OrderStatus.QUEUED.name()).idempotencyKey("k").build();
        when(repo.findById(oid)).thenReturn(Mono.just(o));

        StepVerifier.create(svc.cancel(other, oid))
                .expectErrorMatches(t -> t instanceof BusinessException be && be.getStatus() == 403)
                .verify();
    }

    @Test
    void cancelTerminalRejected() {
        UUID uid = UUID.randomUUID();
        UUID oid = UUID.randomUUID();
        Order o = Order.builder().id(oid).userId(uid).symbol("BTC-USDT")
                .side("BUY").type("LIMIT").quantity(BigDecimal.ONE)
                .status(OrderStatus.FILLED.name()).idempotencyKey("k").build();
        when(repo.findById(oid)).thenReturn(Mono.just(o));

        StepVerifier.create(svc.cancel(uid, oid))
                .expectErrorMatches(t -> t instanceof BusinessException be
                        && be.getCode() == ErrorCode.INVALID_STATE_TRANSITION)
                .verify();
    }

    @Test
    void getReturnsFromCacheWhenPresent() {
        UUID oid = UUID.randomUUID();
        var cached = new com.hft.order.dto.OrderResponse(oid, UUID.randomUUID(), "BTC-USDT",
                "BUY", "LIMIT", BigDecimal.TEN, null, BigDecimal.ONE,
                BigDecimal.ZERO, null, OrderStatus.QUEUED.name(), "k", null,
                OffsetDateTime.now(), OffsetDateTime.now());
        when(cache.get(oid)).thenReturn(Mono.just(cached));

        StepVerifier.create(svc.get(oid))
                .assertNext(r -> { assert r.id().equals(oid); })
                .verifyComplete();
    }

    @Test
    void getFallsBackToRepoOnCacheMiss() {
        UUID oid = UUID.randomUUID();
        Order o = Order.builder().id(oid).userId(UUID.randomUUID()).symbol("BTC-USDT")
                .side("BUY").type("LIMIT").quantity(BigDecimal.ONE)
                .status(OrderStatus.QUEUED.name()).idempotencyKey("k").build();
        when(cache.get(oid)).thenReturn(Mono.empty());
        when(repo.findById(oid)).thenReturn(Mono.just(o));

        StepVerifier.create(svc.get(oid))
                .assertNext(r -> { assert r.id().equals(oid); })
                .verifyComplete();
    }
}
