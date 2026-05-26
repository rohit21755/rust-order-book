package com.hft.marketdata.config;

import com.hft.shared.security.JwtTokenValidator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.web.server.SecurityWebFilterChain;

/**
 * Market data REST + public WS streams are unauthenticated (public market data).
 * The portfolio WS stream authenticates itself via a JWT query param (see PortfolioWebSocketHandler),
 * validated by the shared {@link JwtTokenValidator}.
 */
@Configuration
@EnableWebFluxSecurity
public class SecurityConfig {

    @Bean
    public JwtTokenValidator jwtTokenValidator(MarketDataProperties props) {
        return new JwtTokenValidator(new JwtTokenValidator.Config(
                props.getJwt().getSecret(), props.getJwt().getIssuer()));
    }

    @Bean
    public SecurityWebFilterChain securityFilterChain(ServerHttpSecurity http) {
        http
                .csrf(ServerHttpSecurity.CsrfSpec::disable)
                .httpBasic(ServerHttpSecurity.HttpBasicSpec::disable)
                .formLogin(ServerHttpSecurity.FormLoginSpec::disable)
                .authorizeExchange(ex -> ex
                        // Public market data + WS handshakes; portfolio WS self-authenticates.
                        .pathMatchers("/api/market/**", "/ws/**",
                                "/actuator/health/**", "/actuator/info", "/actuator/prometheus").permitAll()
                        .anyExchange().permitAll());
        return http.build();
    }
}
