package com.hft.auth.controller;

import com.hft.auth.dto.LoginRequest;
import com.hft.auth.dto.RegisterRequest;
import com.hft.auth.dto.TokenResponse;
import com.hft.auth.dto.UserDto;
import com.hft.auth.mapper.UserMapper;
import com.hft.auth.model.User;
import com.hft.auth.security.AuthenticatedUser;
import com.hft.auth.service.AuthService;
import com.hft.auth.service.UserService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AuthControllerTest {

    @Mock AuthService authService;
    @Mock UserService userService;
    @Mock UserMapper userMapper;

    private WebTestClient client() {
        return WebTestClient.bindToController(new AuthController(authService, userService, userMapper))
                .controllerAdvice(new com.hft.auth.exception.GlobalExceptionHandler())
                .build();
    }

    @Test
    void registerReturnsTokens() {
        when(authService.register(eq("a@b.com"), eq("password123"), eq("TRADER")))
                .thenReturn(Mono.just(TokenResponse.bearer("at", "rt", 900)));

        client().post().uri("/auth/register")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(new RegisterRequest("a@b.com", "password123", "TRADER"))
                .exchange()
                .expectStatus().isOk()
                .expectHeader().value("Set-Cookie", s -> s.contains("refresh_token=rt"))
                .expectBody()
                .jsonPath("$.accessToken").isEqualTo("at")
                .jsonPath("$.refreshToken").isEqualTo("rt")
                .jsonPath("$.tokenType").isEqualTo("Bearer");
    }

    @Test
    void loginReturnsTokens() {
        when(authService.login(eq("a@b.com"), eq("password123")))
                .thenReturn(Mono.just(TokenResponse.bearer("at", "rt", 900)));

        client().post().uri("/auth/login")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(new LoginRequest("a@b.com", "password123"))
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.accessToken").isEqualTo("at");
    }

    @Test
    void meReturnsCurrentUser() {
        UUID uid = UUID.randomUUID();
        User u = User.builder().id(uid).email("a@b.com").role("TRADER").enabled(true)
                .createdAt(OffsetDateTime.now()).build();
        when(userService.findById(uid)).thenReturn(Mono.just(u));
        when(userMapper.toDto(u)).thenReturn(new UserDto(uid, "a@b.com", "TRADER", true, u.getCreatedAt()));

        var auth = new AuthenticatedUser(uid, "t", List.of(new SimpleGrantedAuthority("ROLE_TRADER")));

        WebTestClient.bindToController(new AuthController(authService, userService, userMapper))
                .build()
                .mutateWith(org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers
                        .mockAuthentication(auth))
                .get().uri("/auth/me")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.email").isEqualTo("a@b.com")
                .jsonPath("$.role").isEqualTo("TRADER");
    }

    @Test
    void registerRejectsBadEmail() {
        client().post().uri("/auth/register")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(new RegisterRequest("not-an-email", "password123", "TRADER"))
                .exchange()
                .expectStatus().isBadRequest();
    }
}
