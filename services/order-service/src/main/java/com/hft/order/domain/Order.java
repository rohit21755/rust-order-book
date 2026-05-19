package com.hft.order.domain;

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
@Table("orders")
public class Order {

    @Id
    private UUID id;

    @Column("user_id")
    private UUID userId;

    private String symbol;

    private String side; // OrderSide

    private String type; // OrderType

    private BigDecimal price;

    @Column("stop_price")
    private BigDecimal stopPrice;

    private BigDecimal quantity;

    @Column("filled_quantity")
    private BigDecimal filledQuantity;

    @Column("avg_fill_price")
    private BigDecimal avgFillPrice;

    private String status; // OrderStatus

    @Column("idempotency_key")
    private String idempotencyKey;

    @Column("reject_reason")
    private String rejectReason;

    @Column("created_at")
    private OffsetDateTime createdAt;

    @Column("updated_at")
    private OffsetDateTime updatedAt;

    public OrderStatus statusEnum() {
        return OrderStatus.valueOf(status);
    }

    public OrderSide sideEnum() {
        return OrderSide.valueOf(side);
    }

    public OrderType typeEnum() {
        return OrderType.valueOf(type);
    }
}
