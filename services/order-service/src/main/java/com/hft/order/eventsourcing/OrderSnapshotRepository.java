package com.hft.order.eventsourcing;

import io.r2dbc.postgresql.codec.Json;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Repository
public interface OrderSnapshotRepository extends ReactiveCrudRepository<OrderSnapshot, UUID> {

    @Query("""
            INSERT INTO order_snapshots(aggregate_id, version, snapshot_payload, "timestamp")
            VALUES (:aggregateId, :version, :payload::jsonb, NOW())
            ON CONFLICT (aggregate_id) DO UPDATE
                SET version = EXCLUDED.version,
                    snapshot_payload = EXCLUDED.snapshot_payload,
                    "timestamp" = EXCLUDED."timestamp"
            """)
    Mono<Integer> upsert(UUID aggregateId, long version, Json payload);
}
