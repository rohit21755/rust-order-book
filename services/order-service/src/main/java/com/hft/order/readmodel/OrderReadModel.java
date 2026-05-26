package com.hft.order.readmodel;

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
@Table("order_read_model")
public class OrderReadModel {

    @Id
    @Column("order_id")
    private UUID orderId;

    @Column("user_id")
    private UUID userId;

    private String symbol;
    private String side;
    private String type;

    private BigDecimal price;

    @Column("stop_price")
    private BigDecimal stopPrice;

    private BigDecimal quantity;

    @Column("filled_quantity")
    private BigDecimal filledQuantity;

    @Column("avg_fill_price")
    private BigDecimal avgFillPrice;

    private String status;

    @Column("idempotency_key")
    private String idempotencyKey;

    @Column("reject_reason")
    private String rejectReason;

    @Column("last_sequence")
    private long lastSequence;

    @Column("created_at")
    private OffsetDateTime createdAt;

    @Column("updated_at")
    private OffsetDateTime updatedAt;
}
