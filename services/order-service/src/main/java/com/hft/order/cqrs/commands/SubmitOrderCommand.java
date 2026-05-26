package com.hft.order.cqrs.commands;

import com.hft.order.domain.OrderSide;
import com.hft.order.domain.OrderType;

import java.math.BigDecimal;
import java.util.UUID;

public record SubmitOrderCommand(
        UUID aggregateId,
        UUID userId,
        String symbol,
        OrderSide side,
        OrderType type,
        BigDecimal price,
        BigDecimal stopPrice,
        BigDecimal quantity,
        String idempotencyKey
) implements Command {}
