package com.hft.order.eventsourcing.events;

import java.time.OffsetDateTime;
import java.util.UUID;

public record OrderValidatedEvent(UUID aggregateId, OffsetDateTime occurredAt) implements DomainEvent {}
