package com.hft.auth.service;

import com.hft.auth.exception.AuthException;
import com.hft.auth.model.User;
import com.hft.auth.security.JwtUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AuthServiceTest {

    @Mock UserService userService;
    @Mock RefreshTokenService refreshService;
    @Mock JwtUtil jwtUtil;

    @InjectMocks AuthService authService;

    @Test
    void loginIssuesTokenPair() {
        UUID uid = UUID.randomUUID();
        User u = User.builder().id(uid).role("TRADER").build();
        when(userService.authenticate("e@x", "p")).thenReturn(Mono.just(u));
        when(jwtUtil.generateAccessToken(any(), anyList())).thenReturn("access.jwt");
        when(jwtUtil.accessTtlSeconds()).thenReturn(900L);
        when(refreshService.issueAndIndex(uid)).thenReturn(Mono.just("tid.refresh"));

        StepVerifier.create(authService.login("e@x", "p"))
                .assertNext(tr -> {
                    assert tr.accessToken().equals("access.jwt");
                    assert tr.refreshToken().equals("tid.refresh");
                    assert tr.tokenType().equals("Bearer");
                    assert tr.expiresInSeconds() == 900;
                })
                .verifyComplete();
    }

    @Test
    void refreshSucceedsForKnownToken() {
        UUID uid = UUID.randomUUID();
        User u = User.builder().id(uid).role("TRADER").build();
        when(refreshService.validate("tid.refresh")).thenReturn(Mono.just(uid));
        when(userService.findById(uid)).thenReturn(Mono.just(u));
        when(jwtUtil.generateAccessToken(any(), anyList())).thenReturn("new.access");
        when(jwtUtil.accessTtlSeconds()).thenReturn(900L);

        StepVerifier.create(authService.refresh("tid.refresh"))
                .assertNext(tr -> {
                    assert tr.accessToken().equals("new.access");
                    assert tr.refreshToken() == null;
                })
                .verifyComplete();
    }

    @Test
    void refreshFailsForUnknownToken() {
        when(refreshService.validate("bad")).thenReturn(Mono.empty());

        StepVerifier.create(authService.refresh("bad"))
                .expectErrorMatches(t -> t instanceof AuthException ax
                        && ax.getStatus().value() == 401)
                .verify();
    }

    @Test
    void refreshFailsForMissingToken() {
        StepVerifier.create(authService.refresh(null))
                .expectErrorMatches(t -> t instanceof AuthException)
                .verify();
    }

    @Test
    void logoutDelegatesRevoke() {
        when(refreshService.revoke("tid.refresh")).thenReturn(Mono.just(2L));
        StepVerifier.create(authService.logout("tid.refresh")).verifyComplete();
    }

    @Test
    void logoutOnNullIsNoop() {
        StepVerifier.create(authService.logout(null)).verifyComplete();
    }
}
