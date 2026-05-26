package com.hft.portfolio.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("portfolio_holdings")
public class Holding {

    @Id
    private UUID id;

    @Column("user_id")
    private UUID userId;

    private String symbol;

    private BigDecimal quantity;

    @Column("avg_buy_price")
    private BigDecimal avgBuyPrice;

    /** Optimistic-locking version. */
    @Version
    private Long version;

    @Column("updated_at")
    private OffsetDateTime updatedAt;
}
