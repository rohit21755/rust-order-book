package com.hft.order.eventsourcing.events;

import java.time.OffsetDateTime;
import java.util.UUID;

public record OrderQueuedEvent(UUID aggregateId, OffsetDateTime occurredAt) implements DomainEvent {}
