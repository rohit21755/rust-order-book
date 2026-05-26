package com.hft.order.eventsourcing;

import io.r2dbc.postgresql.codec.Json;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.OffsetDateTime;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("order_snapshots")
public class OrderSnapshot implements Persistable<UUID> {

    @Id
    @Column("aggregate_id")
    private UUID aggregateId;

    private long version;

    @Column("snapshot_payload")
    private Json snapshotPayload;

    @Column("timestamp")
    private OffsetDateTime timestamp;

    /**
     * R2DBC has no native upsert; using {@link Persistable#isNew()}=true forces save() → INSERT.
     * Real flow uses a custom upsert query, so this flag is set false to default to UPDATE,
     * and the repository exposes an explicit upsert below.
     */
    @Override public UUID getId() { return aggregateId; }
    @Override public boolean isNew() { return false; }
}
