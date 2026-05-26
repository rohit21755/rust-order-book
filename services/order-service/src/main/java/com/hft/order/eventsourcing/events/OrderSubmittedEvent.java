package com.hft.order.eventsourcing.events;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

public record OrderSubmittedEvent(
        UUID aggregateId,
        UUID userId,
        String symbol,
        String side,
        String type,
        BigDecimal price,
        BigDecimal stopPrice,
        BigDecimal quantity,
        String idempotencyKey,
        OffsetDateTime occurredAt
) implements DomainEvent {}
