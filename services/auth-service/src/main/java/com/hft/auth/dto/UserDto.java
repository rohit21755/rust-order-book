package com.hft.auth.dto;

import java.time.OffsetDateTime;
import java.util.UUID;

public record UserDto(
        UUID id,
        String email,
        String role,
        boolean enabled,
        OffsetDateTime createdAt
) {}
