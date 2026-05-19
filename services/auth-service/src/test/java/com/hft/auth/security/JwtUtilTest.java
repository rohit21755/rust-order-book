package com.hft.auth.security;

import com.auth0.jwt.interfaces.DecodedJWT;
import com.hft.auth.config.AuthProperties;
import com.hft.auth.exception.AuthException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JwtUtilTest {

    private JwtUtil jwtUtil;

    @BeforeEach
    void setUp() {
        AuthProperties props = new AuthProperties();
        props.getJwt().setSecret("test-secret-please-use-32-bytes-min-len!");
        props.getJwt().setIssuer("hft-auth-service");
        jwtUtil = new JwtUtil(props);
    }

    @Test
    void generateAndValidateAccessToken() {
        UUID userId = UUID.randomUUID();
        String token = jwtUtil.generateAccessToken(userId, List.of("TRADER"));

        DecodedJWT decoded = jwtUtil.validateToken(token);
        assertThat(decoded.getSubject()).isEqualTo(userId.toString());
        assertThat(decoded.getClaim("roles").asList(String.class)).containsExactly("TRADER");
        assertThat(decoded.getClaim("type").asString()).isEqualTo("access");
    }

    @Test
    void extractUserIdReturnsSubject() {
        UUID userId = UUID.randomUUID();
        String token = jwtUtil.generateAccessToken(userId, List.of("ADMIN"));
        assertThat(jwtUtil.extractUserId(token)).isEqualTo(userId);
    }

    @Test
    void invalidTokenThrowsUnauthorized() {
        assertThatThrownBy(() -> jwtUtil.validateToken("garbage"))
                .isInstanceOf(AuthException.class)
                .hasMessageContaining("Invalid");
    }

    @Test
    void refreshTokenIsOpaqueAndRandom() {
        String t1 = jwtUtil.generateRefreshToken(UUID.randomUUID());
        String t2 = jwtUtil.generateRefreshToken(UUID.randomUUID());
        assertThat(t1).isNotBlank();
        assertThat(t1).isNotEqualTo(t2);
        // not a JWT (no dots in the base64url payload of >2)
        assertThat(t1.split("\\.").length).isEqualTo(1);
    }
}
