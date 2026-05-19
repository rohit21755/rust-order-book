package com.hft.auth.dto;

public record TokenResponse(
        String accessToken,
        String refreshToken,
        String tokenType,
        long expiresInSeconds
) {
    public static TokenResponse bearer(String access, String refresh, long expires) {
        return new TokenResponse(access, refresh, "Bearer", expires);
    }
}
