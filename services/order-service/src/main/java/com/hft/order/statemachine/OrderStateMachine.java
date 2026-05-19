package com.hft.order.statemachine;

import com.hft.order.domain.OrderStatus;
import com.hft.shared.error.BusinessException;
import com.hft.shared.error.ErrorCode;

import java.util.Map;
import java.util.Set;

/**
 * Manual state machine. Validates legal transitions; throws BusinessException on illegal moves.
 * Transition table:
 *   PENDING_VALIDATION → {VALIDATED, REJECTED, SYSTEM_ERROR}
 *   VALIDATED          → {QUEUED, REJECTED, CANCELLED, SYSTEM_ERROR}
 *   QUEUED             → {PARTIAL_FILL, FILLED, CANCELLED, REJECTED, SYSTEM_ERROR}
 *   PARTIAL_FILL       → {PARTIAL_FILL, FILLED, CANCELLED, SYSTEM_ERROR}
 *   FILLED/CANCELLED/REJECTED/SYSTEM_ERROR → terminal
 */
public final class OrderStateMachine {

    private static final Map<OrderStatus, Set<OrderStatus>> TRANSITIONS = Map.of(
            OrderStatus.PENDING_VALIDATION, Set.of(OrderStatus.VALIDATED, OrderStatus.REJECTED, OrderStatus.SYSTEM_ERROR),
            OrderStatus.VALIDATED, Set.of(OrderStatus.QUEUED, OrderStatus.REJECTED, OrderStatus.CANCELLED, OrderStatus.SYSTEM_ERROR),
            OrderStatus.QUEUED, Set.of(OrderStatus.PARTIAL_FILL, OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.SYSTEM_ERROR),
            OrderStatus.PARTIAL_FILL, Set.of(OrderStatus.PARTIAL_FILL, OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.SYSTEM_ERROR),
            OrderStatus.FILLED, Set.of(),
            OrderStatus.CANCELLED, Set.of(),
            OrderStatus.REJECTED, Set.of(),
            OrderStatus.SYSTEM_ERROR, Set.of()
    );

    private OrderStateMachine() {}

    public static boolean canTransition(OrderStatus from, OrderStatus to) {
        return TRANSITIONS.getOrDefault(from, Set.of()).contains(to);
    }

    public static void requireTransition(OrderStatus from, OrderStatus to) {
        if (!canTransition(from, to)) {
            throw new BusinessException(409, ErrorCode.INVALID_STATE_TRANSITION,
                    "Illegal transition " + from + " → " + to);
        }
    }
}
