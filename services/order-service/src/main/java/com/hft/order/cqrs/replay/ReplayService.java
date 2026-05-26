package com.hft.order.cqrs.replay;

import com.hft.order.cqrs.projection.OrderProjection;
import com.hft.order.eventsourcing.EventStoreService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Replay events of a given aggregate type starting from sequence X. Used to rebuild read
 * models or to bring up a new projection.
 *
 * <p>Idempotent: each event is gated by {@link OrderProjection}'s
 * processed_events ledger; running the same replay twice produces the same read-model state.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ReplayService {

    private final EventStoreService eventStore;
    private final OrderProjection projection;

    /** Replays up to {@code batchSize} events starting at fromSequence. Returns count processed. */
    public Mono<Long> replay(String aggregateType, long fromSequence, int batchSize) {
        log.info("replay start type={} fromSeq={} batchSize={}", aggregateType, fromSequence, batchSize);
        return eventStore.streamByType(aggregateType, fromSequence, batchSize)
                .concatMap(projection::applyRecord)
                .count()
                .doOnNext(n -> log.info("replay complete type={} processed={}", aggregateType, n));
    }
}
