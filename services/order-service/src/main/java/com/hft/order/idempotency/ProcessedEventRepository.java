package com.hft.order.idempotency;

import org.springframework.data.r2dbc.repository.Query;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
public interface ProcessedEventRepository
        extends org.springframework.data.repository.reactive.ReactiveCrudRepository<ProcessedEvent, String> {

    @Query("""
            INSERT INTO processed_events(consumer_group, event_id, processed_at)
            VALUES (:group, :eventId, NOW())
            ON CONFLICT (consumer_group, event_id) DO NOTHING
            """)
    Mono<Integer> tryClaim(String group, String eventId);

    @Query("SELECT EXISTS(SELECT 1 FROM processed_events WHERE consumer_group = :group AND event_id = :eventId)")
    Mono<Boolean> exists(String group, String eventId);
}
