package com.hft.shared.security;

import com.auth0.jwt.interfaces.DecodedJWT;
import org.springframework.security.authentication.ReactiveAuthenticationManager;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class ReactiveJwtAuthenticationManager implements ReactiveAuthenticationManager {

    private final JwtTokenValidator validator;

    public ReactiveJwtAuthenticationManager(JwtTokenValidator validator) {
        this.validator = validator;
    }

    @Override
    public Mono<Authentication> authenticate(Authentication authentication) {
        String token = authentication.getCredentials().toString();
        return Mono.fromCallable(() -> validator.validate(token))
                .map(this::toAuth)
                .onErrorMap(JwtTokenValidator.InvalidTokenException.class,
                        e -> new org.springframework.security.authentication.BadCredentialsException(e.getMessage(), e));
    }

    private Authentication toAuth(DecodedJWT jwt) {
        UUID userId = UUID.fromString(jwt.getSubject());
        List<String> roles = jwt.getClaim("roles").asList(String.class);
        var authorities = (roles == null ? List.<String>of() : roles).stream()
                .map(r -> r.startsWith("ROLE_") ? r : "ROLE_" + r)
                .map(SimpleGrantedAuthority::new)
                .collect(Collectors.toList());
        return new AuthenticatedUserPrincipal(userId, jwt.getToken(), authorities);
    }
}
