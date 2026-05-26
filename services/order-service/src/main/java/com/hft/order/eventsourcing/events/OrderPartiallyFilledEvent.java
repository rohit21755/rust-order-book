package com.hft.order.eventsourcing.events;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

public record OrderPartiallyFilledEvent(
        UUID aggregateId,
        BigDecimal fillQuantity,
        BigDecimal fillPrice,
        BigDecimal remainingQuantity,
        OffsetDateTime occurredAt
) implements DomainEvent {}
