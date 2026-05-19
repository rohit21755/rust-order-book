package com.hft.order.service;

import com.hft.order.domain.Order;
import com.hft.order.domain.OrderStatus;
import com.hft.order.dto.OrderRequest;
import com.hft.order.dto.OrderResponse;
import com.hft.order.kafka.OrderEvent;
import com.hft.order.kafka.OrderEventPublisher;
import com.hft.order.redis.IdempotencyService;
import com.hft.order.redis.OrderCache;
import com.hft.order.repository.OrderRepository;
import com.hft.order.statemachine.OrderStateMachine;
import com.hft.order.validation.OrderValidator;
import com.hft.shared.error.BusinessException;
import com.hft.shared.error.ErrorCode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository repo;
    private final OrderValidator validator;
    private final OrderEventPublisher publisher;
    private final IdempotencyService idempotency;
    private final OrderCache cache;

    public Mono<OrderResponse> submit(UUID userId, OrderRequest req) {
        // 1. structural validation (sync, throws BusinessException)
        validator.validateStructure(req);

        UUID orderId = UUID.randomUUID();

        // 2. reserve idempotency key (atomic SETNX)
        return idempotency.reserve(userId, req.idempotencyKey(), orderId)
                .flatMap(reserved -> {
                    if (!Boolean.TRUE.equals(reserved)) {
                        return idempotency.lookup(userId, req.idempotencyKey())
                                .flatMap(existing -> repo.findById(UUID.fromString(existing))
                                        .switchIfEmpty(Mono.error(BusinessException.conflict(
                                                ErrorCode.DUPLICATE_IDEMPOTENCY_KEY,
                                                "Duplicate idempotency key"))))
                                .map(OrderResponse::from);
                    }
                    return persistAndPublish(userId, orderId, req);
                });
    }

    private Mono<OrderResponse> persistAndPublish(UUID userId, UUID orderId, OrderRequest req) {
        return validator.validateOpenOrderLimit(userId, req.symbol())
                .then(Mono.defer(() -> {
                    Order o = Order.builder()
                            .id(orderId)
                            .userId(userId)
                            .symbol(req.symbol())
                            .side(req.side().name())
                            .type(req.type().name())
                            .price(req.price())
                            .stopPrice(req.stopPrice())
                            .quantity(req.quantity())
                            .filledQuantity(java.math.BigDecimal.ZERO)
                            .status(OrderStatus.PENDING_VALIDATION.name())
                            .idempotencyKey(req.idempotencyKey())
                            .createdAt(OffsetDateTime.now())
                            .updatedAt(OffsetDateTime.now())
                            .build();
                    return repo.save(o);
                }))
                .flatMap(saved -> transitionAndUpdate(saved, OrderStatus.VALIDATED, null))
                .flatMap(saved -> {
                    OrderEvent event = new OrderEvent(
                            OrderEvent.Type.NEW,
                            saved.getId(), saved.getUserId(),
                            saved.getSymbol(), saved.getSide(), saved.getType(),
                            saved.getPrice(), saved.getStopPrice(), saved.getQuantity(),
                            Instant.now(), saved.getIdempotencyKey());
                    return publisher.publish(event)
                            .then(transitionAndUpdate(saved, OrderStatus.QUEUED, null))
                            .onErrorResume(BusinessException.class, err ->
                                    transitionAndUpdate(saved, OrderStatus.SYSTEM_ERROR, err.getMessage())
                                            .then(Mono.error(err)));
                })
                .flatMap(o -> {
                    OrderResponse resp = OrderResponse.from(o);
                    return cache.put(resp).thenReturn(resp);
                });
    }

    public Mono<OrderResponse> cancel(UUID userId, UUID orderId) {
        return repo.findById(orderId)
                .switchIfEmpty(Mono.error(BusinessException.notFound(
                        ErrorCode.ORDER_NOT_FOUND, "Order not found")))
                .flatMap(o -> {
                    if (!o.getUserId().equals(userId)) {
                        return Mono.error(BusinessException.forbidden("Not order owner"));
                    }
                    OrderStateMachine.requireTransition(o.statusEnum(), OrderStatus.CANCELLED);
                    return transitionAndUpdate(o, OrderStatus.CANCELLED, "user requested");
                })
                .flatMap(o -> {
                    OrderEvent ev = new OrderEvent(OrderEvent.Type.CANCEL,
                            o.getId(), o.getUserId(), o.getSymbol(),
                            o.getSide(), o.getType(), o.getPrice(), o.getStopPrice(),
                            o.getQuantity(), Instant.now(), o.getIdempotencyKey());
                    return publisher.publish(ev).thenReturn(o);
                })
                .map(OrderResponse::from)
                .flatMap(resp -> cache.put(resp).thenReturn(resp));
    }

    public Mono<OrderResponse> get(UUID orderId) {
        return cache.get(orderId)
                .switchIfEmpty(repo.findById(orderId)
                        .switchIfEmpty(Mono.error(BusinessException.notFound(
                                ErrorCode.ORDER_NOT_FOUND, "Order not found")))
                        .map(OrderResponse::from));
    }

    public Flux<OrderResponse> search(UUID userId, String symbol, String status, int page, int size) {
        int offset = page * size;
        return repo.search(userId, symbol, status, size, offset).map(OrderResponse::from);
    }

    public Mono<Long> count(UUID userId, String symbol, String status) {
        return repo.count(userId, symbol, status);
    }

    private Mono<Order> transitionAndUpdate(Order o, OrderStatus next, String reason) {
        OrderStateMachine.requireTransition(o.statusEnum(), next);
        return repo.updateStatus(o.getId(), next.name(), reason)
                .thenReturn(applyLocal(o, next, reason));
    }

    private Order applyLocal(Order o, OrderStatus next, String reason) {
        o.setStatus(next.name());
        if (reason != null) o.setRejectReason(reason);
        o.setUpdatedAt(OffsetDateTime.now());
        return o;
    }
}
