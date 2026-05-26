package com.hft.order.cqrs;

import com.hft.order.cqrs.commands.CancelOrderCommand;
import com.hft.order.cqrs.commands.SubmitOrderCommand;
import com.hft.order.domain.OrderStatus;
import com.hft.order.dto.OrderResponse;
import com.hft.order.eventsourcing.AggregateLoader;
import com.hft.order.eventsourcing.EventStoreService;
import com.hft.order.eventsourcing.OrderAggregate;
import com.hft.order.eventsourcing.SnapshotService;
import com.hft.order.eventsourcing.events.DomainEvent;
import com.hft.order.eventsourcing.events.OrderCancelledEvent;
import com.hft.order.eventsourcing.events.OrderQueuedEvent;
import com.hft.order.eventsourcing.events.OrderSubmittedEvent;
import com.hft.order.eventsourcing.events.OrderSystemErrorEvent;
import com.hft.order.eventsourcing.events.OrderValidatedEvent;
import com.hft.order.kafka.OrderEvent;
import com.hft.order.kafka.OrderEventPublisher;
import com.hft.order.redis.IdempotencyService;
import com.hft.order.validation.OrderValidator;
import com.hft.shared.error.BusinessException;
import com.hft.shared.error.ErrorCode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Write-side: turns commands into events, appends to event_store, publishes to Kafka.
 *
 * <p>Within a single command:
 * <ol>
 *   <li>Validate the command (sync + async).</li>
 *   <li>Reserve idempotency key in Redis (SETNX).</li>
 *   <li>Append events to event_store in one R2DBC tx (monotonic seq enforced by UNIQUE).</li>
 *   <li>Publish corresponding Kafka {@link OrderEvent} (after commit).</li>
 *   <li>Save snapshot if version % 50 == 0.</li>
 * </ol>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderCommandHandler {

    private final EventStoreService eventStore;
    private final SnapshotService snapshots;
    private final AggregateLoader loader;
    private final OrderValidator validator;
    private final IdempotencyService idempotency;
    private final OrderEventPublisher kafkaPublisher;
    private final TransactionalOperator txOperator;

    /** Submit: appends Submitted → Validated → Queued (or Rejected) and publishes NEW order. */
    public Mono<OrderResponse> handle(SubmitOrderCommand cmd) {
        var dto = toRequest(cmd);
        try {
            validator.validateStructure(dto);
        } catch (BusinessException be) {
            return Mono.error(be);
        }

        UUID aggregateId = cmd.aggregateId();
        return idempotency.reserve(cmd.userId(), cmd.idempotencyKey(), aggregateId)
                .flatMap(reserved -> {
                    if (!Boolean.TRUE.equals(reserved)) {
                        return Mono.error(BusinessException.conflict(
                                ErrorCode.DUPLICATE_IDEMPOTENCY_KEY, "Duplicate idempotency key"));
                    }
                    return validator.validateOpenOrderLimit(cmd.userId(), cmd.symbol()).then(persistSubmit(cmd, dto));
                });
    }

    private Mono<OrderResponse> persistSubmit(SubmitOrderCommand cmd,
                                              com.hft.order.dto.OrderRequest dto) {
        OffsetDateTime now = OffsetDateTime.now();
        List<DomainEvent> events = List.of(
                new OrderSubmittedEvent(cmd.aggregateId(), cmd.userId(), cmd.symbol(),
                        cmd.side().name(), cmd.type().name(),
                        cmd.price(), cmd.stopPrice(), cmd.quantity(),
                        cmd.idempotencyKey(), now),
                new OrderValidatedEvent(cmd.aggregateId(), now),
                new OrderQueuedEvent(cmd.aggregateId(), now)
        );

        Mono<OrderAggregate> dbWork = appendAll(cmd.aggregateId(), 0L, events);

        return dbWork.as(txOperator::transactional)
                .flatMap(agg -> publishKafkaNew(agg, cmd)
                        .onErrorResume(err -> persistSystemError(agg, err).then(Mono.error(err)))
                        .thenReturn(agg))
                .flatMap(agg -> snapshots.takeIfDue(agg).thenReturn(toResponse(agg)));
    }

    /** Cancel: append OrderCancelled to the existing aggregate. */
    public Mono<OrderResponse> handle(CancelOrderCommand cmd) {
        return loader.load(cmd.aggregateId())
                .flatMap(agg -> {
                    if (agg.getStatus() == null) {
                        return Mono.error(BusinessException.notFound(ErrorCode.ORDER_NOT_FOUND, "Order not found"));
                    }
                    if (!cmd.userId().equals(agg.getUserId())) {
                        return Mono.error(BusinessException.forbidden("Not order owner"));
                    }
                    if (agg.getStatus().isTerminal()) {
                        return Mono.error(BusinessException.conflict(ErrorCode.INVALID_STATE_TRANSITION,
                                "Order already terminal"));
                    }
                    DomainEvent ev = new OrderCancelledEvent(cmd.aggregateId(),
                            cmd.reason() == null ? "user requested" : cmd.reason(),
                            OffsetDateTime.now());
                    return appendAll(cmd.aggregateId(), agg.getVersion(), List.of(ev))
                            .as(txOperator::transactional)
                            .flatMap(updated -> publishKafkaCancel(updated).thenReturn(updated))
                            .flatMap(updated -> snapshots.takeIfDue(updated).thenReturn(toResponse(updated)));
                });
    }

    private Mono<OrderAggregate> appendAll(UUID aggregateId, long baseVersion, List<DomainEvent> events) {
        OrderAggregate agg = new OrderAggregate();
        agg.setVersion(baseVersion);
        Mono<OrderAggregate> chain = Mono.just(agg);
        long seq = baseVersion;
        for (DomainEvent ev : events) {
            final long next = ++seq;
            chain = chain.flatMap(a -> eventStore.append(aggregateId, next, ev).thenReturn(a))
                    .doOnNext(a -> a.apply(ev));
        }
        return chain;
    }

    private Mono<Void> publishKafkaNew(OrderAggregate agg, SubmitOrderCommand cmd) {
        OrderEvent ev = new OrderEvent(
                OrderEvent.Type.NEW, agg.getId(), agg.getUserId(),
                agg.getSymbol(), agg.getSide(), agg.getType(),
                agg.getPrice(), agg.getStopPrice(), agg.getQuantity(),
                Instant.now(), agg.getIdempotencyKey());
        return kafkaPublisher.publish(ev);
    }

    private Mono<Void> publishKafkaCancel(OrderAggregate agg) {
        OrderEvent ev = new OrderEvent(
                OrderEvent.Type.CANCEL, agg.getId(), agg.getUserId(),
                agg.getSymbol(), agg.getSide(), agg.getType(),
                agg.getPrice(), agg.getStopPrice(), agg.getQuantity(),
                Instant.now(), agg.getIdempotencyKey());
        return kafkaPublisher.publish(ev);
    }

    private Mono<Void> persistSystemError(OrderAggregate agg, Throwable err) {
        DomainEvent ev = new OrderSystemErrorEvent(agg.getId(),
                err.getMessage() == null ? "publish_failed" : err.getMessage(),
                OffsetDateTime.now());
        long next = agg.getVersion() + 1;
        return eventStore.append(agg.getId(), next, ev).then();
    }

    private static com.hft.order.dto.OrderRequest toRequest(SubmitOrderCommand cmd) {
        return new com.hft.order.dto.OrderRequest(cmd.symbol(), cmd.side(), cmd.type(),
                cmd.price(), cmd.stopPrice(), cmd.quantity(), cmd.idempotencyKey());
    }

    private static OrderResponse toResponse(OrderAggregate agg) {
        OrderStatus s = agg.getStatus();
        return new OrderResponse(
                agg.getId(), agg.getUserId(), agg.getSymbol(), agg.getSide(), agg.getType(),
                agg.getPrice(), agg.getStopPrice(), agg.getQuantity(),
                agg.getFilledQuantity() == null ? BigDecimal.ZERO : agg.getFilledQuantity(),
                agg.getAvgFillPrice(),
                s == null ? null : s.name(),
                agg.getIdempotencyKey(), agg.getRejectReason(),
                agg.getCreatedAt(), agg.getUpdatedAt());
    }
}
