package com.hft.order.idempotency;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.OffsetDateTime;

/**
 * Composite-key entity is awkward in R2DBC; we use the standalone repository
 * with custom INSERT/SELECT instead of CrudRepository.save.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("processed_events")
public class ProcessedEvent {

    @Id
    @Column("consumer_group")
    private String consumerGroup;

    @Column("event_id")
    private String eventId;

    @Column("processed_at")
    private OffsetDateTime processedAt;
}
