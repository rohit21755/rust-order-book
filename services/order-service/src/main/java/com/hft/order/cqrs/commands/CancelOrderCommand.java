package com.hft.order.cqrs.commands;

import java.util.UUID;

public record CancelOrderCommand(UUID aggregateId, UUID userId, String reason) implements Command {}
