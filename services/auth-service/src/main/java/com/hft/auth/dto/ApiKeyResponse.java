package com.hft.auth.dto;

import java.time.OffsetDateTime;
import java.util.UUID;

public record ApiKeyResponse(
        UUID id,
        String keyPrefix,
        String label,
        String apiKey,           // null on list; populated ONCE on creation
        boolean revoked,
        OffsetDateTime createdAt,
        OffsetDateTime lastUsedAt
) {}
