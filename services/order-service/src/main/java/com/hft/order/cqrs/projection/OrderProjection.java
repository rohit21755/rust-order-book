package com.hft.order.cqrs.projection;

import com.hft.order.eventsourcing.EventRecord;
import com.hft.order.eventsourcing.EventStoreService;
import com.hft.order.eventsourcing.OrderAggregate;
import com.hft.order.idempotency.IdempotentProcessor;
import com.hft.order.readmodel.OrderReadModel;
import com.hft.order.readmodel.OrderReadModelRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Updates the read model from event_store. Two paths:
 *
 * <p><b>Live</b>: any time an aggregate changes (e.g. inside the command handler) → call
 * {@link #project(java.util.UUID)} to rebuild that aggregate into the read model.
 *
 * <p><b>Replay</b>: {@code ReplayService} streams events and calls {@link #applyRecord} per
 * record. Idempotency: processed_events keyed by consumer_group + event_id.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProjection {

    public static final String CONSUMER_GROUP = "order-projection";

    private final EventStoreService eventStore;
    private final OrderReadModelRepository readRepo;
    private final IdempotentProcessor idempotent;

    /** Re-derive an aggregate from event store + upsert into the read model. */
    public Mono<Void> project(java.util.UUID aggregateId) {
        return eventStore.events(aggregateId).collectList()
                .flatMap(records -> {
                    if (records.isEmpty()) return Mono.empty();
                    OrderAggregate agg = new OrderAggregate();
                    agg.replay(eventStore.decodeAll(records));
                    return upsert(agg, records.get(records.size() - 1).getSequenceNumber());
                });
    }

    /** Apply a single event record (used by replay engine). */
    public Mono<Void> applyRecord(EventRecord rec) {
        return idempotent.process(CONSUMER_GROUP, rec.getEventId().toString(), () ->
                eventStore.events(rec.getAggregateId()).collectList()
                        .flatMap(all -> {
                            OrderAggregate agg = new OrderAggregate();
                            agg.replay(eventStore.decodeAll(all));
                            long seq = all.isEmpty() ? rec.getSequenceNumber()
                                    : all.get(all.size() - 1).getSequenceNumber();
                            return upsert(agg, seq);
                        }));
    }

    private Mono<Void> upsert(OrderAggregate agg, long sequence) {
        OrderReadModel rm = OrderReadModel.builder()
                .orderId(agg.getId()).userId(agg.getUserId())
                .symbol(agg.getSymbol()).side(agg.getSide()).type(agg.getType())
                .price(agg.getPrice()).stopPrice(agg.getStopPrice()).quantity(agg.getQuantity())
                .filledQuantity(agg.getFilledQuantity()).avgFillPrice(agg.getAvgFillPrice())
                .status(agg.getStatus() == null ? null : agg.getStatus().name())
                .idempotencyKey(agg.getIdempotencyKey()).rejectReason(agg.getRejectReason())
                .lastSequence(sequence)
                .createdAt(agg.getCreatedAt()).updatedAt(agg.getUpdatedAt())
                .build();
        return readRepo.upsert(rm.getOrderId(), rm.getUserId(), rm.getSymbol(), rm.getSide(), rm.getType(),
                        rm.getPrice(), rm.getStopPrice(), rm.getQuantity(),
                        rm.getFilledQuantity(), rm.getAvgFillPrice(), rm.getStatus(),
                        rm.getIdempotencyKey(), rm.getRejectReason(), rm.getLastSequence(),
                        rm.getCreatedAt(), rm.getUpdatedAt())
                .doOnNext(n -> log.debug("projected aggregate={} seq={} rows={}",
                        rm.getOrderId(), rm.getLastSequence(), n))
                .then();
    }
}
