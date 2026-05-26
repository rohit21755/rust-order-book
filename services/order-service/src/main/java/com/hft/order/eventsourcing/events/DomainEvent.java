package com.hft.order.eventsourcing.events;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Immutable domain event. Persisted to event_store as JSONB (payload) keyed by
 * (aggregate_id, sequence_number). Event store is append-only.
 *
 * <p>The {@code @JsonTypeInfo} block keeps the on-wire payload self-describing so the
 * projection / replay path can route to the right concrete type without out-of-band data.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@type", include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
        @JsonSubTypes.Type(value = OrderSubmittedEvent.class, name = "OrderSubmitted"),
        @JsonSubTypes.Type(value = OrderValidatedEvent.class, name = "OrderValidated"),
        @JsonSubTypes.Type(value = OrderQueuedEvent.class, name = "OrderQueued"),
        @JsonSubTypes.Type(value = OrderPartiallyFilledEvent.class, name = "OrderPartiallyFilled"),
        @JsonSubTypes.Type(value = OrderFilledEvent.class, name = "OrderFilled"),
        @JsonSubTypes.Type(value = OrderCancelledEvent.class, name = "OrderCancelled"),
        @JsonSubTypes.Type(value = OrderRejectedEvent.class, name = "OrderRejected"),
        @JsonSubTypes.Type(value = OrderSystemErrorEvent.class, name = "OrderSystemError"),
})
public interface DomainEvent {
    /** Aggregate (order) id. */
    UUID aggregateId();

    /** Event time-of-emit. */
    OffsetDateTime occurredAt();

    /** Stable wire type used by projection/idempotency. */
    default String eventType() {
        return getClass().getSimpleName().replace("Event", "");
    }
}
