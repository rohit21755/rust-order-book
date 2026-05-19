package com.hft.auth.model;

public enum Role {
    ADMIN,
    TRADER,
    READ_ONLY;

    public String authority() {
        return "ROLE_" + name();
    }
}
