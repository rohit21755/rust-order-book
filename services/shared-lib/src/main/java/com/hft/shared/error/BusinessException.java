package com.hft.shared.error;

public class BusinessException extends RuntimeException {
    private final int status;
    private final ErrorCode code;

    public BusinessException(int status, ErrorCode code, String message) {
        super(message);
        this.status = status;
        this.code = code;
    }

    public int getStatus() { return status; }
    public ErrorCode getCode() { return code; }

    public static BusinessException badRequest(ErrorCode code, String msg) { return new BusinessException(400, code, msg); }
    public static BusinessException unauthorized(String msg) { return new BusinessException(401, ErrorCode.UNAUTHORIZED, msg); }
    public static BusinessException forbidden(String msg) { return new BusinessException(403, ErrorCode.FORBIDDEN, msg); }
    public static BusinessException notFound(ErrorCode code, String msg) { return new BusinessException(404, code, msg); }
    public static BusinessException conflict(ErrorCode code, String msg) { return new BusinessException(409, code, msg); }
    public static BusinessException tooMany(String msg) { return new BusinessException(429, ErrorCode.RATE_LIMIT_EXCEEDED, msg); }
    public static BusinessException internal(ErrorCode code, String msg) { return new BusinessException(500, code, msg); }
}
