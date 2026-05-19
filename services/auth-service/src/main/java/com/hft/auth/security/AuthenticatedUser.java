package com.hft.auth.security;

import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;

import java.util.Collection;
import java.util.UUID;

public class AuthenticatedUser extends AbstractAuthenticationToken {

    private final UUID userId;
    private final String credentials;

    public AuthenticatedUser(UUID userId, String credentials, Collection<? extends GrantedAuthority> authorities) {
        super(authorities);
        this.userId = userId;
        this.credentials = credentials;
        setAuthenticated(true);
    }

    @Override
    public Object getCredentials() {
        return credentials;
    }

    @Override
    public Object getPrincipal() {
        return userId;
    }

    public UUID getUserId() {
        return userId;
    }
}
