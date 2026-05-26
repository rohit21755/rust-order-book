package com.hft.order.eventsourcing;

import com.hft.order.domain.OrderStatus;
import com.hft.order.eventsourcing.events.DomainEvent;
import com.hft.order.eventsourcing.events.OrderCancelledEvent;
import com.hft.order.eventsourcing.events.OrderFilledEvent;
import com.hft.order.eventsourcing.events.OrderPartiallyFilledEvent;
import com.hft.order.eventsourcing.events.OrderQueuedEvent;
import com.hft.order.eventsourcing.events.OrderRejectedEvent;
import com.hft.order.eventsourcing.events.OrderSubmittedEvent;
import com.hft.order.eventsourcing.events.OrderSystemErrorEvent;
import com.hft.order.eventsourcing.events.OrderValidatedEvent;
import com.hft.order.statemachine.OrderStateMachine;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Pure aggregate. State is rebuilt by folding a sequence of {@link DomainEvent}s.
 * No I/O, no Spring deps — safe for replay + tests.
 */
@Data
@NoArgsConstructor
public class OrderAggregate {

    private UUID id;
    private UUID userId;
    private String symbol;
    private String side;
    private String type;
    private BigDecimal price;
    private BigDecimal stopPrice;
    private BigDecimal quantity;
    private BigDecimal filledQuantity = BigDecimal.ZERO;
    private BigDecimal avgFillPrice;
    private OrderStatus status;
    private String idempotencyKey;
    private String rejectReason;
    private OffsetDateTime createdAt;
    private OffsetDateTime updatedAt;
    private long version; // last applied event sequence

    /** Replay a list of events; throws on illegal state transitions. */
    public OrderAggregate replay(List<? extends DomainEvent> events) {
        for (DomainEvent e : events) apply(e);
        return this;
    }

    public void apply(DomainEvent e) {
        switch (e) {
            case OrderSubmittedEvent ev -> applySubmitted(ev);
            case OrderValidatedEvent ev -> transition(OrderStatus.VALIDATED, ev.occurredAt());
            case OrderQueuedEvent ev -> transition(OrderStatus.QUEUED, ev.occurredAt());
            case OrderPartiallyFilledEvent ev -> applyPartialFill(ev);
            case OrderFilledEvent ev -> applyFilled(ev);
            case OrderCancelledEvent ev -> applyCancelled(ev);
            case OrderRejectedEvent ev -> applyRejected(ev);
            case OrderSystemErrorEvent ev -> applySystemError(ev);
            default -> throw new IllegalStateException("Unknown event type: " + e.getClass().getName());
        }
        this.updatedAt = e.occurredAt();
        this.version++;
    }

    private void applySubmitted(OrderSubmittedEvent ev) {
        if (status != null) throw new IllegalStateException("OrderSubmitted on existing aggregate " + id);
        this.id = ev.aggregateId();
        this.userId = ev.userId();
        this.symbol = ev.symbol();
        this.side = ev.side();
        this.type = ev.type();
        this.price = ev.price();
        this.stopPrice = ev.stopPrice();
        this.quantity = ev.quantity();
        this.filledQuantity = BigDecimal.ZERO;
        this.idempotencyKey = ev.idempotencyKey();
        this.status = OrderStatus.PENDING_VALIDATION;
        this.createdAt = ev.occurredAt();
    }

    private void applyPartialFill(OrderPartiallyFilledEvent ev) {
        OrderStateMachine.requireTransition(status, OrderStatus.PARTIAL_FILL);
        BigDecimal prevFilled = filledQuantity == null ? BigDecimal.ZERO : filledQuantity;
        BigDecimal prevAvg = avgFillPrice == null ? BigDecimal.ZERO : avgFillPrice;
        BigDecimal newFilled = prevFilled.add(ev.fillQuantity());
        BigDecimal weighted = prevFilled.multiply(prevAvg).add(ev.fillQuantity().multiply(ev.fillPrice()));
        this.avgFillPrice = newFilled.signum() == 0 ? BigDecimal.ZERO
                : weighted.divide(newFilled, 8, RoundingMode.HALF_UP);
        this.filledQuantity = newFilled;
        this.status = OrderStatus.PARTIAL_FILL;
    }

    private void applyFilled(OrderFilledEvent ev) {
        OrderStateMachine.requireTransition(status, OrderStatus.FILLED);
        this.filledQuantity = ev.totalFilled();
        this.avgFillPrice = ev.avgFillPrice();
        this.status = OrderStatus.FILLED;
    }

    private void applyCancelled(OrderCancelledEvent ev) {
        OrderStateMachine.requireTransition(status, OrderStatus.CANCELLED);
        this.status = OrderStatus.CANCELLED;
        this.rejectReason = ev.reason();
    }

    private void applyRejected(OrderRejectedEvent ev) {
        OrderStateMachine.requireTransition(status, OrderStatus.REJECTED);
        this.status = OrderStatus.REJECTED;
        this.rejectReason = ev.reason();
    }

    private void applySystemError(OrderSystemErrorEvent ev) {
        OrderStateMachine.requireTransition(status, OrderStatus.SYSTEM_ERROR);
        this.status = OrderStatus.SYSTEM_ERROR;
        this.rejectReason = ev.reason();
    }

    private void transition(OrderStatus next, OffsetDateTime at) {
        OrderStateMachine.requireTransition(status, next);
        this.status = next;
        this.updatedAt = at;
    }
}
