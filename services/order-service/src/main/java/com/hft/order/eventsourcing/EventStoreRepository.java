package com.hft.order.eventsourcing;

import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Repository
public interface EventStoreRepository extends ReactiveCrudRepository<EventRecord, UUID> {

    @Query("SELECT * FROM event_store WHERE aggregate_id = :aggregateId ORDER BY sequence_number ASC")
    Flux<EventRecord> findByAggregate(UUID aggregateId);

    @Query("""
            SELECT * FROM event_store
            WHERE aggregate_id = :aggregateId AND sequence_number > :afterSeq
            ORDER BY sequence_number ASC
            """)
    Flux<EventRecord> findByAggregateAfter(UUID aggregateId, long afterSeq);

    @Query("""
            SELECT * FROM event_store
            WHERE aggregate_type = :type AND sequence_number >= :fromSeq
            ORDER BY "timestamp" ASC, sequence_number ASC
            LIMIT :limit
            """)
    Flux<EventRecord> stream(String type, long fromSeq, int limit);

    @Query("SELECT COALESCE(MAX(sequence_number), 0) FROM event_store WHERE aggregate_id = :aggregateId")
    Mono<Long> currentVersion(UUID aggregateId);
}
