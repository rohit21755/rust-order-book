package com.hft.portfolio.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("portfolio_pnl_records")
public class PnlRecord {

    @Id
    private UUID id;

    @Column("user_id")
    private UUID userId;

    private String symbol;

    @Column("realized_pnl")
    private BigDecimal realizedPnl;

    @Column("unrealized_pnl")
    private BigDecimal unrealizedPnl;

    private OffsetDateTime timestamp;
}
