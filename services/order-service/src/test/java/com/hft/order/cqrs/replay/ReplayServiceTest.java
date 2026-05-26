package com.hft.order.cqrs.replay;

import com.hft.order.cqrs.projection.OrderProjection;
import com.hft.order.eventsourcing.EventRecord;
import com.hft.order.eventsourcing.EventStoreService;
import io.r2dbc.postgresql.codec.Json;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.OffsetDateTime;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReplayServiceTest {

    EventStoreService store;
    OrderProjection projection;
    ReplayService service;

    @BeforeEach
    void setUp() {
        store = mock(EventStoreService.class);
        projection = mock(OrderProjection.class);
        service = new ReplayService(store, projection);
    }

    @Test
    void replaysEachEventOnce() {
        EventRecord r1 = sampleRecord(1);
        EventRecord r2 = sampleRecord(2);
        when(store.streamByType(anyString(), anyLong(), anyInt())).thenReturn(Flux.just(r1, r2));
        when(projection.applyRecord(any())).thenReturn(Mono.empty());

        StepVerifier.create(service.replay("ORDER", 0, 1000)).expectNext(2L).verifyComplete();
        verify(projection, times(2)).applyRecord(any());
    }

    @Test
    void replayProducesSameResultOnSecondRun() {
        EventRecord r1 = sampleRecord(1);
        when(store.streamByType(anyString(), anyLong(), anyInt())).thenReturn(Flux.just(r1)).thenReturn(Flux.just(r1));
        // applyRecord is itself idempotent (uses processed_events).
        when(projection.applyRecord(any())).thenReturn(Mono.empty());

        StepVerifier.create(service.replay("ORDER", 0, 1000)).expectNext(1L).verifyComplete();
        StepVerifier.create(service.replay("ORDER", 0, 1000)).expectNext(1L).verifyComplete();
    }

    private EventRecord sampleRecord(long seq) {
        return EventRecord.builder()
                .eventId(UUID.randomUUID())
                .aggregateId(UUID.randomUUID())
                .aggregateType("ORDER")
                .eventType("OrderSubmitted")
                .sequenceNumber(seq)
                .payload(Json.of("{}"))
                .metadata(Json.of("{}"))
                .timestamp(OffsetDateTime.now())
                .build();
    }
}
