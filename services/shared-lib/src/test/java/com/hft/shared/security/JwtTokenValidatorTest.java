package com.hft.shared.security;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JwtTokenValidatorTest {

    private static final String SECRET = "shared-test-secret-minimum-32-bytes!";
    private static final String ISSUER = "hft-auth-service";

    private final JwtTokenValidator validator = new JwtTokenValidator(new JwtTokenValidator.Config(SECRET, ISSUER));

    @Test
    void validatesSignedToken() {
        UUID uid = UUID.randomUUID();
        String token = JWT.create()
                .withIssuer(ISSUER)
                .withSubject(uid.toString())
                .withClaim("roles", List.of("TRADER"))
                .withIssuedAt(new Date())
                .withExpiresAt(new Date(System.currentTimeMillis() + 60_000))
                .sign(Algorithm.HMAC256(SECRET.getBytes(StandardCharsets.UTF_8)));

        assertThat(validator.extractUserId(token)).isEqualTo(uid);
        assertThat(validator.extractRoles(token)).containsExactly("TRADER");
    }

    @Test
    void rejectsBadSignature() {
        String token = JWT.create()
                .withIssuer(ISSUER)
                .withSubject(UUID.randomUUID().toString())
                .withExpiresAt(new Date(System.currentTimeMillis() + 60_000))
                .sign(Algorithm.HMAC256("wrong-secret-32-bytes-too-short-padding!".getBytes()));

        assertThatThrownBy(() -> validator.validate(token))
                .isInstanceOf(JwtTokenValidator.InvalidTokenException.class);
    }

    @Test
    void rejectsExpired() {
        String token = JWT.create()
                .withIssuer(ISSUER)
                .withSubject(UUID.randomUUID().toString())
                .withExpiresAt(new Date(System.currentTimeMillis() - 1000))
                .sign(Algorithm.HMAC256(SECRET.getBytes()));

        assertThatThrownBy(() -> validator.validate(token))
                .isInstanceOf(JwtTokenValidator.InvalidTokenException.class);
    }
}
