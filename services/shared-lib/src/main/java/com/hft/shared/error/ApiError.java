package com.hft.shared.error;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Structured error payload returned by every service. */
public record ApiError(
        Instant timestamp,
        int status,
        ErrorCode code,
        String message,
        String path,
        List<FieldViolation> violations,
        Map<String, Object> details
) {
    public static ApiError of(int status, ErrorCode code, String message, String path) {
        return new ApiError(Instant.now(), status, code, message, path, List.of(), Map.of());
    }

    public record FieldViolation(String field, String message) {}
}
