package com.hft.auth.exception;

import org.springframework.http.HttpStatus;

public class AuthException extends RuntimeException {
    private final HttpStatus status;

    public AuthException(HttpStatus status, String message) {
        super(message);
        this.status = status;
    }

    public HttpStatus getStatus() {
        return status;
    }

    public static AuthException unauthorized(String msg) {
        return new AuthException(HttpStatus.UNAUTHORIZED, msg);
    }

    public static AuthException badRequest(String msg) {
        return new AuthException(HttpStatus.BAD_REQUEST, msg);
    }

    public static AuthException forbidden(String msg) {
        return new AuthException(HttpStatus.FORBIDDEN, msg);
    }

    public static AuthException conflict(String msg) {
        return new AuthException(HttpStatus.CONFLICT, msg);
    }

    public static AuthException tooManyRequests(String msg) {
        return new AuthException(HttpStatus.TOO_MANY_REQUESTS, msg);
    }

    public static AuthException notFound(String msg) {
        return new AuthException(HttpStatus.NOT_FOUND, msg);
    }
}
