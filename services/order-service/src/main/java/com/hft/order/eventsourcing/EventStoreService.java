package com.hft.order.eventsourcing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.order.eventsourcing.events.DomainEvent;
import io.r2dbc.postgresql.codec.Json;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Append-only event store facade.
 *
 * <p>Append uses {@code event_store.aggregate_id + sequence_number} unique constraint to enforce
 * monotonic increasing sequence; concurrent appends collide → {@link org.springframework.dao.DuplicateKeyException}
 * which callers may treat as an optimistic-concurrency violation and retry.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EventStoreService {

    public static final String AGGREGATE_TYPE = "ORDER";

    private final EventStoreRepository repo;
    private final ObjectMapper objectMapper;

    public Mono<EventRecord> append(UUID aggregateId, long sequence, DomainEvent event) {
        return Mono.fromCallable(() -> objectMapper.writeValueAsString(event))
                .map(Json::of)
                .flatMap(payload -> {
                    EventRecord rec = EventRecord.builder()
                            .eventId(UUID.randomUUID())
                            .aggregateId(aggregateId)
                            .aggregateType(AGGREGATE_TYPE)
                            .eventType(event.eventType())
                            .sequenceNumber(sequence)
                            .payload(payload)
                            .metadata(Json.of("{}"))
                            .timestamp(OffsetDateTime.now())
                            .build();
                    return repo.save(rec);
                });
    }

    public Mono<Long> currentVersion(UUID aggregateId) {
        return repo.currentVersion(aggregateId).defaultIfEmpty(0L);
    }

    public Flux<EventRecord> events(UUID aggregateId) {
        return repo.findByAggregate(aggregateId);
    }

    public Flux<EventRecord> eventsAfter(UUID aggregateId, long version) {
        return repo.findByAggregateAfter(aggregateId, version);
    }

    public Flux<EventRecord> streamByType(String aggregateType, long fromSeq, int limit) {
        return repo.stream(aggregateType, fromSeq, limit);
    }

    /** Deserialize a stored event back into its concrete {@link DomainEvent} subtype. */
    public DomainEvent decode(EventRecord rec) {
        try {
            return objectMapper.readValue(rec.getPayload().asString(), DomainEvent.class);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to decode event " + rec.getEventId(), e);
        }
    }

    /** Convenience: decode a batch and return as {@code List<DomainEvent>}. */
    public List<DomainEvent> decodeAll(List<EventRecord> records) {
        return records.stream().map(this::decode).toList();
    }
}
