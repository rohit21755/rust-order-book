package com.hft.shared.security;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTVerificationException;
import com.auth0.jwt.interfaces.DecodedJWT;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

/**
 * HS256 JWT verifier intended to be shared by all resource servers
 * (Order Service, Portfolio Service, etc.). Reads access tokens issued by the Auth Service.
 *
 * Configure via {@link Config} with the same secret + issuer as the Auth Service.
 */
public class JwtTokenValidator {

    public record Config(String secret, String issuer) {}

    private final JWTVerifier verifier;

    public JwtTokenValidator(Config config) {
        Algorithm algorithm = Algorithm.HMAC256(config.secret().getBytes(StandardCharsets.UTF_8));
        this.verifier = JWT.require(algorithm).withIssuer(config.issuer()).build();
    }

    /** Verify token; throws InvalidTokenException on failure. */
    public DecodedJWT validate(String token) {
        try {
            return verifier.verify(token);
        } catch (JWTVerificationException ex) {
            throw new InvalidTokenException(ex.getMessage(), ex);
        }
    }

    public UUID extractUserId(String token) {
        return UUID.fromString(validate(token).getSubject());
    }

    public List<String> extractRoles(String token) {
        var claim = validate(token).getClaim("roles");
        return claim == null ? List.of() : claim.asList(String.class);
    }

    public static class InvalidTokenException extends RuntimeException {
        public InvalidTokenException(String msg, Throwable cause) { super(msg, cause); }
    }
}
