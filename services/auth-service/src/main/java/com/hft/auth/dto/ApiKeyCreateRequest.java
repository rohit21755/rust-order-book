package com.hft.auth.dto;

import jakarta.validation.constraints.Size;

public record ApiKeyCreateRequest(@Size(max = 255) String label) {}
