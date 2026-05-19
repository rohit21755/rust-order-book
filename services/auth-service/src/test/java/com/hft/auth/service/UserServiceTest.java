package com.hft.auth.service;

import com.hft.auth.exception.AuthException;
import com.hft.auth.model.User;
import com.hft.auth.repository.UserRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.crypto.password.PasswordEncoder;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.OffsetDateTime;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UserServiceTest {

    @Mock UserRepository repo;
    @Mock PasswordEncoder encoder;

    @InjectMocks UserService service;

    @Test
    void registerNewUser() {
        when(repo.existsByEmail("a@b.com")).thenReturn(Mono.just(false));
        when(encoder.encode(anyString())).thenReturn("hashed");
        when(repo.save(any(User.class))).thenAnswer(inv -> {
            User u = inv.getArgument(0);
            u.setId(UUID.randomUUID());
            return Mono.just(u);
        });

        StepVerifier.create(service.register("a@b.com", "password123", "TRADER"))
                .assertNext(u -> {
                    assert u.getEmail().equals("a@b.com");
                    assert u.getPasswordHash().equals("hashed");
                    assert u.getRole().equals("TRADER");
                })
                .verifyComplete();
    }

    @Test
    void registerDuplicateEmailConflicts() {
        when(repo.existsByEmail("dup@b.com")).thenReturn(Mono.just(true));

        StepVerifier.create(service.register("dup@b.com", "password123", "TRADER"))
                .expectErrorMatches(t -> t instanceof AuthException ax
                        && ax.getStatus().value() == 409)
                .verify();
    }

    @Test
    void authenticateSucceedsWhenPasswordMatches() {
        User u = User.builder()
                .id(UUID.randomUUID())
                .email("a@b.com")
                .passwordHash("hashed")
                .role("TRADER")
                .enabled(true)
                .createdAt(OffsetDateTime.now())
                .build();
        when(repo.findByEmail("a@b.com")).thenReturn(Mono.just(u));
        when(encoder.matches(eq("password123"), eq("hashed"))).thenReturn(true);

        StepVerifier.create(service.authenticate("a@b.com", "password123"))
                .expectNext(u)
                .verifyComplete();
    }

    @Test
    void authenticateFailsOnBadPassword() {
        User u = User.builder().passwordHash("hashed").enabled(true).build();
        when(repo.findByEmail("a@b.com")).thenReturn(Mono.just(u));
        when(encoder.matches(anyString(), anyString())).thenReturn(false);

        StepVerifier.create(service.authenticate("a@b.com", "wrong"))
                .expectErrorMatches(t -> t instanceof AuthException ax
                        && ax.getStatus().value() == 401)
                .verify();
    }

    @Test
    void authenticateFailsWhenUserDisabled() {
        User u = User.builder().passwordHash("hashed").enabled(false).build();
        when(repo.findByEmail("a@b.com")).thenReturn(Mono.just(u));
        when(encoder.matches(anyString(), anyString())).thenReturn(true);

        StepVerifier.create(service.authenticate("a@b.com", "password123"))
                .expectErrorMatches(t -> t instanceof AuthException)
                .verify();
    }

    @Test
    void invalidRoleRejected() {
        when(repo.existsByEmail("a@b.com")).thenReturn(Mono.just(false));
        StepVerifier.create(service.register("a@b.com", "password123", "WIZARD"))
                .expectErrorMatches(t -> t instanceof AuthException ax
                        && ax.getStatus().value() == 400)
                .verify();
    }
}
