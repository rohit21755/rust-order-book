package com.hft.portfolio.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.OffsetDateTime;

/**
 * Idempotency ledger: presence of a tradeId means the trade was already settled.
 * Implements {@link Persistable} so R2DBC treats it as an INSERT (no SELECT-then-update),
 * letting a PK conflict surface as a duplicate.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("processed_trades")
public class ProcessedTrade implements Persistable<String> {

    @Id
    @Column("trade_id")
    private String tradeId;

    @Column("processed_at")
    private OffsetDateTime processedAt;

    @Override
    public String getId() {
        return tradeId;
    }

    @Override
    public boolean isNew() {
        return true; // always insert
    }
}
