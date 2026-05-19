package com.hft.auth.service;

import com.hft.auth.exception.AuthException;
import com.hft.auth.model.Role;
import com.hft.auth.model.User;
import com.hft.auth.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.OffsetDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class UserService {

    private final UserRepository repo;
    private final PasswordEncoder encoder;

    public Mono<User> register(String email, String rawPassword, String roleStr) {
        Role role = parseRole(roleStr);
        return repo.existsByEmail(email)
                .flatMap(exists -> exists
                        ? Mono.error(AuthException.conflict("Email already registered"))
                        : hash(rawPassword).flatMap(hash -> {
                            User u = User.builder()
                                    .email(email)
                                    .passwordHash(hash)
                                    .role(role.name())
                                    .enabled(true)
                                    .createdAt(OffsetDateTime.now())
                                    .updatedAt(OffsetDateTime.now())
                                    .build();
                            return repo.save(u);
                        }));
    }

    public Mono<User> authenticate(String email, String rawPassword) {
        return repo.findByEmail(email)
                .switchIfEmpty(Mono.error(AuthException.unauthorized("Invalid credentials")))
                .flatMap(u -> matches(rawPassword, u.getPasswordHash())
                        .flatMap(ok -> ok && u.isEnabled()
                                ? Mono.just(u)
                                : Mono.error(AuthException.unauthorized("Invalid credentials"))));
    }

    public Mono<User> findById(UUID id) {
        return repo.findById(id)
                .switchIfEmpty(Mono.error(AuthException.notFound("User not found")));
    }

    private Mono<String> hash(String raw) {
        return Mono.fromCallable(() -> encoder.encode(raw)).subscribeOn(Schedulers.boundedElastic());
    }

    private Mono<Boolean> matches(String raw, String hash) {
        return Mono.fromCallable(() -> encoder.matches(raw, hash)).subscribeOn(Schedulers.boundedElastic());
    }

    private Role parseRole(String s) {
        if (s == null || s.isBlank()) return Role.READ_ONLY;
        try {
            return Role.valueOf(s.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw AuthException.badRequest("Invalid role: " + s);
        }
    }
}
