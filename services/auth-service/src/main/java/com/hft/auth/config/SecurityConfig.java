package com.hft.auth.config;

import com.hft.auth.security.ApiKeyAuthenticationFilter;
import com.hft.auth.security.BearerTokenConverter;
import com.hft.auth.security.JwtAuthenticationManager;
import com.hft.auth.security.RateLimitWebFilter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.method.configuration.EnableReactiveMethodSecurity;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.SecurityWebFiltersOrder;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.oauth2.jwt.NimbusReactiveJwtDecoder;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtReactiveAuthenticationManager;
import org.springframework.security.web.server.SecurityWebFilterChain;
import org.springframework.security.web.server.authentication.AuthenticationWebFilter;
import org.springframework.security.web.server.context.NoOpServerSecurityContextRepository;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.reactive.CorsConfigurationSource;
import org.springframework.web.cors.reactive.UrlBasedCorsConfigurationSource;

import java.util.Arrays;
import java.util.List;

@Configuration
@EnableWebFluxSecurity
@EnableReactiveMethodSecurity
public class SecurityConfig {

    @Bean
    public SecurityWebFilterChain securityFilterChain(
            ServerHttpSecurity http,
            JwtAuthenticationManager jwtAuthManager,
            BearerTokenConverter bearerConverter,
            ApiKeyAuthenticationFilter apiKeyFilter,
            RateLimitWebFilter rateLimitFilter,
            CorsConfigurationSource cors
    ) {
        AuthenticationWebFilter jwtFilter = new AuthenticationWebFilter(jwtAuthManager);
        jwtFilter.setServerAuthenticationConverter(bearerConverter);
        jwtFilter.setSecurityContextRepository(NoOpServerSecurityContextRepository.getInstance());

        http
                .csrf(ServerHttpSecurity.CsrfSpec::disable)
                .cors(c -> c.configurationSource(cors))
                .httpBasic(ServerHttpSecurity.HttpBasicSpec::disable)
                .formLogin(ServerHttpSecurity.FormLoginSpec::disable)
                .securityContextRepository(NoOpServerSecurityContextRepository.getInstance())
                .authorizeExchange(ex -> ex
                        .pathMatchers(
                                "/auth/register",
                                "/auth/login",
                                "/auth/refresh",
                                "/actuator/health/**",
                                "/actuator/info",
                                "/actuator/prometheus"
                        ).permitAll()
                        .pathMatchers("/auth/admin/**").hasRole("ADMIN")
                        .anyExchange().authenticated()
                )
                // Order: API key filter runs BEFORE JWT auth filter; then rate-limit applies to authenticated principal.
                .addFilterAt(apiKeyFilter, SecurityWebFiltersOrder.AUTHENTICATION)
                .addFilterAt(jwtFilter, SecurityWebFiltersOrder.AUTHENTICATION)
                .addFilterAfter(rateLimitFilter, SecurityWebFiltersOrder.AUTHORIZATION);

        return http.build();
    }

    /**
     * OAuth2 Resource Server JWT decoder — accepts Google id-tokens via JWK set URI (lazy fetch).
     * Only registered when google.enabled=true AND jwk-set-uri configured.
     * GitHub access tokens are opaque; would require an introspection decoder (not exposed here).
     */
    @Bean
    @ConditionalOnProperty(prefix = "auth.oauth-providers.google",
            name = {"enabled", "jwk-set-uri"}, matchIfMissing = false)
    public ReactiveJwtDecoder googleJwtDecoder(AuthProperties props) {
        return NimbusReactiveJwtDecoder
                .withJwkSetUri(props.getOauthProviders().getGoogle().getJwkSetUri())
                .build();
    }

    @Bean
    @ConditionalOnProperty(prefix = "auth.oauth-providers.google",
            name = {"enabled", "jwk-set-uri"}, matchIfMissing = false)
    public JwtReactiveAuthenticationManager oauth2AuthenticationManager(ReactiveJwtDecoder decoder) {
        return new JwtReactiveAuthenticationManager(decoder);
    }

    @Bean
    public CorsConfigurationSource corsConfigurationSource(AuthProperties props) {
        CorsConfiguration cfg = new CorsConfiguration();
        cfg.setAllowedOrigins(props.getCors().getAllowedOrigins());
        cfg.setAllowedMethods(Arrays.asList("GET", "POST", "PUT", "DELETE", "OPTIONS"));
        cfg.setAllowedHeaders(List.of("*"));
        cfg.setAllowCredentials(true);
        cfg.setMaxAge(3600L);
        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", cfg);
        return source;
    }
}
