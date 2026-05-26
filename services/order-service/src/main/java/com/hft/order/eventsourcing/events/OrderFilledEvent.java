package com.hft.order.eventsourcing.events;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

public record OrderFilledEvent(
        UUID aggregateId,
        BigDecimal totalFilled,
        BigDecimal avgFillPrice,
        OffsetDateTime occurredAt
) implements DomainEvent {}
