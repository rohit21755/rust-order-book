package com.hft.shared.error;

/** Stable error codes consumed by clients. */
public enum ErrorCode {
    // 400-class
    VALIDATION_FAILED,
    INVALID_SYMBOL,
    INVALID_PRICE,
    INVALID_QUANTITY,
    INVALID_ORDER_TYPE,
    INVALID_STATE_TRANSITION,
    DUPLICATE_IDEMPOTENCY_KEY,
    OPEN_ORDER_LIMIT_EXCEEDED,
    POSITION_LIMIT_EXCEEDED,

    // 401/403
    UNAUTHORIZED,
    FORBIDDEN,

    // 404
    ORDER_NOT_FOUND,
    SYMBOL_NOT_FOUND,

    // 409
    CONFLICT,

    // 429
    RATE_LIMIT_EXCEEDED,

    // 5xx
    KAFKA_PUBLISH_FAILED,
    INTERNAL_ERROR
}
