package com.hft.order.eventsourcing.events;

import java.time.OffsetDateTime;
import java.util.UUID;

public record OrderCancelledEvent(UUID aggregateId, String reason, OffsetDateTime occurredAt) implements DomainEvent {}
