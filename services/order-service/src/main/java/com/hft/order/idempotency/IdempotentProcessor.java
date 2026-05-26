package com.hft.order.idempotency;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.function.Supplier;

/**
 * Idempotency wrapper used by every Kafka consumer.
 *
 * <p>Order of operations:
 * <ol>
 *   <li>{@code tryClaim} → INSERT row keyed by {@code (consumer_group, event_id)};
 *       UPSERT-on-conflict returns 0 rows when already processed.</li>
 *   <li>If newly claimed → invoke {@code work}. On work failure → claim row stays;
 *       the consumer must NOT commit the offset, so the broker redelivers and the
 *       second pass will see the claim row, retry the work, then commit. The
 *       caller is responsible for making {@code work} itself idempotent if it
 *       has external side effects.</li>
 *   <li>If already claimed → skip work, return empty (consumer commits offset).</li>
 * </ol>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class IdempotentProcessor {

    private final ProcessedEventRepository repo;

    public Mono<Void> process(String consumerGroup, String eventId, Supplier<Mono<Void>> work) {
        return repo.tryClaim(consumerGroup, eventId)
                .flatMap(rows -> {
                    if (rows == null || rows == 0) {
                        log.debug("event {} already processed by {}, skipping", eventId, consumerGroup);
                        return Mono.empty();
                    }
                    return work.get();
                });
    }
}
