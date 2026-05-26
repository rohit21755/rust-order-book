package com.hft.order.cqrs.commands;

import java.util.UUID;

/** Marker for write-side commands. Each command targets one aggregate id. */
public sealed interface Command permits SubmitOrderCommand, CancelOrderCommand {
    UUID aggregateId();
}
