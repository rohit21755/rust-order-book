package com.hft.order.exception;

import com.hft.shared.error.ApiError;
import com.hft.shared.error.BusinessException;
import com.hft.shared.error.ErrorCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(BusinessException.class)
    public Mono<ResponseEntity<ApiError>> handleBusiness(BusinessException ex, ServerWebExchange ex2) {
        return Mono.just(ResponseEntity.status(ex.getStatus())
                .body(ApiError.of(ex.getStatus(), ex.getCode(), ex.getMessage(), path(ex2))));
    }

    @ExceptionHandler(WebExchangeBindException.class)
    public Mono<ResponseEntity<ApiError>> handleValidation(WebExchangeBindException ex, ServerWebExchange ex2) {
        List<ApiError.FieldViolation> viol = ex.getFieldErrors().stream()
                .map(f -> new ApiError.FieldViolation(f.getField(), f.getDefaultMessage()))
                .toList();
        ApiError body = new ApiError(Instant.now(), 400, ErrorCode.VALIDATION_FAILED,
                "Validation failed", path(ex2), viol, Map.of());
        return Mono.just(ResponseEntity.badRequest().body(body));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public Mono<ResponseEntity<ApiError>> handleIllegalArg(IllegalArgumentException ex, ServerWebExchange ex2) {
        return Mono.just(ResponseEntity.badRequest().body(
                ApiError.of(400, ErrorCode.VALIDATION_FAILED, ex.getMessage(), path(ex2))));
    }

    @ExceptionHandler(Exception.class)
    public Mono<ResponseEntity<ApiError>> handleAny(Exception ex, ServerWebExchange ex2) {
        log.error("Unhandled", ex);
        return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(
                ApiError.of(500, ErrorCode.INTERNAL_ERROR, "Internal error", path(ex2))));
    }

    private String path(ServerWebExchange ex) {
        return ex == null ? null : ex.getRequest().getPath().value();
    }
}
