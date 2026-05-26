package com.hft.order.eventsourcing;

import com.hft.order.domain.OrderStatus;
import com.hft.order.eventsourcing.events.DomainEvent;
import com.hft.order.eventsourcing.events.OrderCancelledEvent;
import com.hft.order.eventsourcing.events.OrderFilledEvent;
import com.hft.order.eventsourcing.events.OrderPartiallyFilledEvent;
import com.hft.order.eventsourcing.events.OrderQueuedEvent;
import com.hft.order.eventsourcing.events.OrderSubmittedEvent;
import com.hft.order.eventsourcing.events.OrderValidatedEvent;
import com.hft.shared.error.BusinessException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderAggregateTest {

    private final UUID id = UUID.randomUUID();
    private final UUID userId = UUID.randomUUID();
    private final OffsetDateTime now = OffsetDateTime.now();

    private DomainEvent submitted() {
        return new OrderSubmittedEvent(id, userId, "BTC-USDT", "BUY", "LIMIT",
                new BigDecimal("100"), null, new BigDecimal("2"), "idem", now);
    }

    @Test
    void replayProducesFinalState() {
        OrderAggregate agg = new OrderAggregate().replay(List.of(
                submitted(),
                new OrderValidatedEvent(id, now),
                new OrderQueuedEvent(id, now),
                new OrderPartiallyFilledEvent(id, new BigDecimal("1"), new BigDecimal("101"),
                        new BigDecimal("1"), now),
                new OrderFilledEvent(id, new BigDecimal("2"), new BigDecimal("101.5"), now)
        ));
        assertThat(agg.getStatus()).isEqualTo(OrderStatus.FILLED);
        assertThat(agg.getFilledQuantity()).isEqualByComparingTo("2");
        assertThat(agg.getAvgFillPrice()).isEqualByComparingTo("101.5");
        assertThat(agg.getVersion()).isEqualTo(5);
    }

    @Test
    void replayIsIdempotentFromSnapshot() {
        OrderAggregate a = new OrderAggregate().replay(List.of(
                submitted(),
                new OrderValidatedEvent(id, now),
                new OrderQueuedEvent(id, now)));
        OrderAggregate b = new OrderAggregate().replay(List.of(
                submitted(),
                new OrderValidatedEvent(id, now),
                new OrderQueuedEvent(id, now)));
        assertThat(a.getStatus()).isEqualTo(b.getStatus());
        assertThat(a.getVersion()).isEqualTo(b.getVersion());
    }

    @Test
    void cancelAfterTerminalThrows() {
        OrderAggregate agg = new OrderAggregate().replay(List.of(
                submitted(),
                new OrderValidatedEvent(id, now),
                new OrderQueuedEvent(id, now),
                new OrderFilledEvent(id, new BigDecimal("2"), new BigDecimal("100"), now)));
        assertThatThrownBy(() -> agg.apply(new OrderCancelledEvent(id, "late", now)))
                .isInstanceOf(BusinessException.class);
    }

    @Test
    void submittedOnExistingAggregateThrows() {
        OrderAggregate agg = new OrderAggregate().replay(List.of(submitted()));
        assertThatThrownBy(() -> agg.apply(submitted())).isInstanceOf(IllegalStateException.class);
    }
}
