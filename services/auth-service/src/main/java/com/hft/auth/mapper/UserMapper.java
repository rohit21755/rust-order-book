package com.hft.auth.mapper;

import com.hft.auth.dto.ApiKeyResponse;
import com.hft.auth.dto.UserDto;
import com.hft.auth.model.ApiKey;
import com.hft.auth.model.User;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

@Mapper(componentModel = "spring")
public interface UserMapper {
    UserDto toDto(User user);

    @Mapping(target = "apiKey", ignore = true)
    ApiKeyResponse toApiKeyResponse(ApiKey key);

    default ApiKeyResponse toApiKeyResponseWithSecret(ApiKey key, String plain) {
        return new ApiKeyResponse(
                key.getId(),
                key.getKeyPrefix(),
                key.getLabel(),
                plain,
                key.isRevoked(),
                key.getCreatedAt(),
                key.getLastUsedAt()
        );
    }
}
