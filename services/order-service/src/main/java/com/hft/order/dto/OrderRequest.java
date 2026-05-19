package com.hft.order.dto;

import com.hft.order.domain.OrderSide;
import com.hft.order.domain.OrderType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

public record OrderRequest(
        @NotBlank String symbol,
        @NotNull OrderSide side,
        @NotNull OrderType type,
        BigDecimal price,
        BigDecimal stopPrice,
        @NotNull @Positive BigDecimal quantity,
        @NotBlank String idempotencyKey
) {}
