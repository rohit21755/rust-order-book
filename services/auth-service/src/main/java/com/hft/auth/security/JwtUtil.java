package com.hft.auth.security;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTVerificationException;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.hft.auth.config.AuthProperties;
import com.hft.auth.exception.AuthException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.UUID;

@Slf4j
@Component
public class JwtUtil {

    private final AuthProperties props;
    private final Algorithm algorithm;
    private final JWTVerifier verifier;
    private final SecureRandom secureRandom = new SecureRandom();

    @Autowired
    public JwtUtil(AuthProperties props) {
        this.props = props;
        byte[] secret = props.getJwt().getSecret().getBytes(StandardCharsets.UTF_8);
        this.algorithm = Algorithm.HMAC256(secret);
        this.verifier = JWT.require(algorithm)
                .withIssuer(props.getJwt().getIssuer())
                .build();
    }

    /** signed JWT — userId subject, roles claim, 15-min TTL by default. */
    public String generateAccessToken(UUID userId, List<String> roles) {
        Instant now = Instant.now();
        Instant exp = now.plus(Duration.ofMinutes(props.getJwt().getAccessTokenTtlMinutes()));
        return JWT.create()
                .withIssuer(props.getJwt().getIssuer())
                .withSubject(userId.toString())
                .withClaim("roles", roles)
                .withClaim("type", "access")
                .withIssuedAt(Date.from(now))
                .withExpiresAt(Date.from(exp))
                .withJWTId(UUID.randomUUID().toString())
                .sign(algorithm);
    }

    /** Opaque (random) refresh token. Caller stores in Redis with TTL. */
    public String generateRefreshToken(UUID userId) {
        byte[] buf = new byte[48];
        secureRandom.nextBytes(buf);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(buf);
    }

    /** Returns DecodedJWT or throws AuthException(401). */
    public DecodedJWT validateToken(String token) {
        try {
            return verifier.verify(token);
        } catch (JWTVerificationException ex) {
            log.debug("JWT validation failed: {}", ex.getMessage());
            throw AuthException.unauthorized("Invalid or expired token");
        }
    }

    public UUID extractUserId(String token) {
        return UUID.fromString(validateToken(token).getSubject());
    }

    public List<String> extractRoles(String token) {
        return validateToken(token).getClaim("roles").asList(String.class);
    }

    public long accessTtlSeconds() {
        return Duration.ofMinutes(props.getJwt().getAccessTokenTtlMinutes()).toSeconds();
    }

    public Duration refreshTtl() {
        return Duration.ofDays(props.getJwt().getRefreshTokenTtlDays());
    }
}
