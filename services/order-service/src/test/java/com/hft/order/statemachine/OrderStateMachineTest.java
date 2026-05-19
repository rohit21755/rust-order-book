package com.hft.order.statemachine;

import com.hft.order.domain.OrderStatus;
import com.hft.shared.error.BusinessException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderStateMachineTest {

    @Test
    void happyPath() {
        assertThat(OrderStateMachine.canTransition(OrderStatus.PENDING_VALIDATION, OrderStatus.VALIDATED)).isTrue();
        assertThat(OrderStateMachine.canTransition(OrderStatus.VALIDATED, OrderStatus.QUEUED)).isTrue();
        assertThat(OrderStateMachine.canTransition(OrderStatus.QUEUED, OrderStatus.PARTIAL_FILL)).isTrue();
        assertThat(OrderStateMachine.canTransition(OrderStatus.PARTIAL_FILL, OrderStatus.FILLED)).isTrue();
    }

    @Test
    void cancellationAllowedFromOpenStates() {
        assertThat(OrderStateMachine.canTransition(OrderStatus.VALIDATED, OrderStatus.CANCELLED)).isTrue();
        assertThat(OrderStateMachine.canTransition(OrderStatus.QUEUED, OrderStatus.CANCELLED)).isTrue();
        assertThat(OrderStateMachine.canTransition(OrderStatus.PARTIAL_FILL, OrderStatus.CANCELLED)).isTrue();
    }

    @Test
    void terminalStatesDoNotTransition() {
        assertThat(OrderStateMachine.canTransition(OrderStatus.FILLED, OrderStatus.CANCELLED)).isFalse();
        assertThat(OrderStateMachine.canTransition(OrderStatus.CANCELLED, OrderStatus.QUEUED)).isFalse();
        assertThat(OrderStateMachine.canTransition(OrderStatus.REJECTED, OrderStatus.VALIDATED)).isFalse();
    }

    @Test
    void requireTransitionThrowsOnIllegal() {
        assertThatThrownBy(() -> OrderStateMachine.requireTransition(OrderStatus.FILLED, OrderStatus.QUEUED))
                .isInstanceOf(BusinessException.class);
    }
}
