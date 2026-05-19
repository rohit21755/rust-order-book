package com.hft.auth.security;

import com.auth0.jwt.interfaces.DecodedJWT;
import lombok.RequiredArgsConstructor;
import org.springframework.security.authentication.ReactiveAuthenticationManager;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class JwtAuthenticationManager implements ReactiveAuthenticationManager {

    private final JwtUtil jwtUtil;

    @Override
    public Mono<Authentication> authenticate(Authentication authentication) {
        String token = authentication.getCredentials().toString();
        return Mono.fromCallable(() -> jwtUtil.validateToken(token))
                .map(this::toAuth);
    }

    private Authentication toAuth(DecodedJWT jwt) {
        UUID userId = UUID.fromString(jwt.getSubject());
        List<String> roles = jwt.getClaim("roles").asList(String.class);
        var authorities = (roles == null ? List.<String>of() : roles).stream()
                .map(r -> r.startsWith("ROLE_") ? r : "ROLE_" + r)
                .map(SimpleGrantedAuthority::new)
                .collect(Collectors.toList());
        return new AuthenticatedUser(userId, jwt.getToken(), authorities);
    }
}
