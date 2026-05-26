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
@Table("portfolio_trades")
public class UserTrade {

    @Id
    private UUID id;

    @Column("user_id")
    private UUID userId;

    @Column("trade_id")
    private String tradeId;

    private String symbol;

    private String side; // BUY / SELL

    private BigDecimal price;

    private BigDecimal quantity;

    @Column("realized_pnl")
    private BigDecimal realizedPnl;

    @Column("executed_at")
    private OffsetDateTime executedAt;
}
