package com.hft.order.domain;

public enum OrderStatus {
    PENDING_VALIDATION,
    VALIDATED,
    QUEUED,
    PARTIAL_FILL,
    FILLED,
    CANCELLED,
    REJECTED,
    SYSTEM_ERROR;

    public boolean isTerminal() {
        return this == FILLED || this == CANCELLED || this == REJECTED || this == SYSTEM_ERROR;
    }

    public boolean isOpen() {
        return this == VALIDATED || this == QUEUED || this == PARTIAL_FILL || this == PENDING_VALIDATION;
    }
}
