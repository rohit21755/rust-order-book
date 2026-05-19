package com.hft.shared.security;

import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;

import java.util.Collection;
import java.util.UUID;

public class AuthenticatedUserPrincipal extends AbstractAuthenticationToken {

    private final UUID userId;
    private final String token;

    public AuthenticatedUserPrincipal(UUID userId, String token, Collection<? extends GrantedAuthority> authorities) {
        super(authorities);
        this.userId = userId;
        this.token = token;
        setAuthenticated(true);
    }

    @Override public Object getCredentials() { return token; }
    @Override public Object getPrincipal() { return userId; }
    public UUID getUserId() { return userId; }
}
