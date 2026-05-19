package com.hft.auth.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.OffsetDateTime;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("api_keys")
public class ApiKey {

    @Id
    private UUID id;

    @Column("user_id")
    private UUID userId;

    @Column("key_hash")
    private String keyHash;

    @Column("key_prefix")
    private String keyPrefix;

    private String label;

    private boolean revoked;

    @Column("created_at")
    private OffsetDateTime createdAt;

    @Column("last_used_at")
    private OffsetDateTime lastUsedAt;
}
