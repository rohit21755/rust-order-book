package com.hft.order.eventsourcing;

import io.r2dbc.postgresql.codec.Json;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.OffsetDateTime;
import java.util.UUID;

/** R2DBC mapping for the {@code event_store} table. JSONB columns are read as Postgres JSON. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("event_store")
public class EventRecord {

    @Id
    @Column("event_id")
    private UUID eventId;

    @Column("aggregate_id")
    private UUID aggregateId;

    @Column("aggregate_type")
    private String aggregateType;

    @Column("event_type")
    private String eventType;

    @Column("sequence_number")
    private long sequenceNumber;

    private Json payload;

    private Json metadata;

    @Column("timestamp")
    private OffsetDateTime timestamp;
}
