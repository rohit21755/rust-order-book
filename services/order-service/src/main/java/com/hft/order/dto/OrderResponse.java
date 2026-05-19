package com.hft.order.dto;

import com.hft.order.domain.Order;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

public record OrderResponse(
        UUID id,
        UUID userId,
        String symbol,
        String side,
        String type,
        BigDecimal price,
        BigDecimal stopPrice,
        BigDecimal quantity,
        BigDecimal filledQuantity,
        BigDecimal avgFillPrice,
        String status,
        String idempotencyKey,
        String rejectReason,
        OffsetDateTime createdAt,
        OffsetDateTime updatedAt
) {
    public static OrderResponse from(Order o) {
        return new OrderResponse(
                o.getId(), o.getUserId(), o.getSymbol(), o.getSide(), o.getType(),
                o.getPrice(), o.getStopPrice(), o.getQuantity(),
                o.getFilledQuantity(), o.getAvgFillPrice(),
                o.getStatus(), o.getIdempotencyKey(), o.getRejectReason(),
                o.getCreatedAt(), o.getUpdatedAt()
        );
    }
}
