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
@Table("users")
public class User {

    @Id
    private UUID id;

    private String email;

    @Column("password_hash")
    private String passwordHash;

    private String role;

    private boolean enabled;

    @Column("created_at")
    private OffsetDateTime createdAt;

    @Column("updated_at")
    private OffsetDateTime updatedAt;

    public Role roleEnum() {
        try {
            return Role.valueOf(role);
        } catch (Exception e) {
            return Role.READ_ONLY;
        }
    }
}
