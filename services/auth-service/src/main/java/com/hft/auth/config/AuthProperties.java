package com.hft.auth.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Data
@Configuration
@ConfigurationProperties(prefix = "auth")
public class AuthProperties {

    private Jwt jwt = new Jwt();
    private RateLimit rateLimit = new RateLimit();
    private Session session = new Session();
    private ApiKey apiKey = new ApiKey();
    private OauthProviders oauthProviders = new OauthProviders();
    private Cors cors = new Cors();

    @Data
    public static class Jwt {
        private String secret;
        private String issuer = "hft-auth-service";
        private int accessTokenTtlMinutes = 15;
        private int refreshTokenTtlDays = 7;
    }

    @Data
    public static class RateLimit {
        private int requestsPerMinute = 100;
        private int windowSeconds = 60;
    }

    @Data
    public static class Session {
        private int ttlMinutes = 30;
    }

    @Data
    public static class ApiKey {
        private String prefix = "hft_";
        private int lengthBytes = 32;
    }

    @Data
    public static class OauthProviders {
        private Provider google = new Provider();
        private Provider github = new Provider();
    }

    @Data
    public static class Provider {
        private boolean enabled = true;
        private String jwkSetUri;
        private String issuer;
    }

    @Data
    public static class Cors {
        private List<String> allowedOrigins = new ArrayList<>();
    }
}
