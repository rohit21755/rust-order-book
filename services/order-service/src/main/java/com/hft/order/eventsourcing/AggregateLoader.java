package com.hft.order.eventsourcing;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.UUID;

/** Loads an {@link OrderAggregate} = (latest snapshot ?: fresh) + replay of events after that version. */
@Service
@RequiredArgsConstructor
public class AggregateLoader {

    private final SnapshotService snapshots;
    private final EventStoreService eventStore;

    public Mono<OrderAggregate> load(UUID aggregateId) {
        return snapshots.loadSnapshot(aggregateId)
                .defaultIfEmpty(new OrderAggregate())
                .flatMap(base -> eventStore.eventsAfter(aggregateId, base.getVersion())
                        .collectList()
                        .map(records -> {
                            base.replay(eventStore.decodeAll(records));
                            return base;
                        }));
    }
}
