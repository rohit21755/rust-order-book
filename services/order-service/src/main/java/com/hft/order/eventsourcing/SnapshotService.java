package com.hft.order.eventsourcing;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.r2dbc.postgresql.codec.Json;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.UUID;

/**
 * Snapshot every N events. Snapshot stored as JSONB blob of the aggregate state.
 *
 * <p>Load path: read latest snapshot (if any) → reconstruct aggregate → replay events with
 * sequence > snapshot.version. Replay-from-scratch is always valid (snapshots are a perf hint).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SnapshotService {

    public static final int SNAPSHOT_EVERY = 50;

    private final OrderSnapshotRepository repo;
    private final ObjectMapper objectMapper;

    public Mono<OrderAggregate> loadSnapshot(UUID aggregateId) {
        return repo.findById(aggregateId)
                .map(this::deserialize);
    }

    public Mono<Void> takeIfDue(OrderAggregate aggregate) {
        if (aggregate.getVersion() == 0 || aggregate.getVersion() % SNAPSHOT_EVERY != 0) {
            return Mono.empty();
        }
        return saveSnapshot(aggregate);
    }

    public Mono<Void> saveSnapshot(OrderAggregate aggregate) {
        return Mono.fromCallable(() -> objectMapper.writeValueAsString(aggregate))
                .map(Json::of)
                .flatMap(json -> repo.upsert(aggregate.getId(), aggregate.getVersion(), json))
                .doOnSuccess(n -> log.debug("snapshot saved aggregate={} version={}",
                        aggregate.getId(), aggregate.getVersion()))
                .then();
    }

    private OrderAggregate deserialize(OrderSnapshot snap) {
        try {
            OrderAggregate agg = objectMapper.readValue(snap.getSnapshotPayload().asString(), OrderAggregate.class);
            agg.setVersion(snap.getVersion());
            return agg;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to deserialize snapshot for " + snap.getAggregateId(), e);
        }
    }
}
