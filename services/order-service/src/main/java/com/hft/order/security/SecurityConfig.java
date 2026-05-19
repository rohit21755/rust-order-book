package com.hft.order.security;

import com.hft.order.config.OrderProperties;
import com.hft.shared.security.BearerAuthenticationConverter;
import com.hft.shared.security.JwtTokenValidator;
import com.hft.shared.security.ReactiveJwtAuthenticationManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.security.config.annotation.method.configuration.EnableReactiveMethodSecurity;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.SecurityWebFiltersOrder;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.web.server.SecurityWebFilterChain;
import org.springframework.security.web.server.ServerAuthenticationEntryPoint;
import org.springframework.security.web.server.authentication.AuthenticationWebFilter;
import org.springframework.security.web.server.context.NoOpServerSecurityContextRepository;
import reactor.core.publisher.Mono;

@Configuration
@EnableWebFluxSecurity
@EnableReactiveMethodSecurity
public class SecurityConfig {

    @Bean
    public JwtTokenValidator jwtTokenValidator(OrderProperties props) {
        return new JwtTokenValidator(new JwtTokenValidator.Config(
                props.getJwt().getSecret(), props.getJwt().getIssuer()));
    }

    @Bean
    public ReactiveJwtAuthenticationManager reactiveJwtAuthenticationManager(JwtTokenValidator v) {
        return new ReactiveJwtAuthenticationManager(v);
    }

    @Bean
    public SecurityWebFilterChain securityFilterChain(
            ServerHttpSecurity http,
            ReactiveJwtAuthenticationManager manager
    ) {
        AuthenticationWebFilter jwtFilter = new AuthenticationWebFilter(manager);
        jwtFilter.setServerAuthenticationConverter(new BearerAuthenticationConverter());
        jwtFilter.setSecurityContextRepository(NoOpServerSecurityContextRepository.getInstance());

        ServerAuthenticationEntryPoint entryPoint = (exchange, ex) -> {
            exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
            return Mono.empty();
        };

        http
                .csrf(ServerHttpSecurity.CsrfSpec::disable)
                .httpBasic(ServerHttpSecurity.HttpBasicSpec::disable)
                .formLogin(ServerHttpSecurity.FormLoginSpec::disable)
                .securityContextRepository(NoOpServerSecurityContextRepository.getInstance())
                .exceptionHandling(eh -> eh.authenticationEntryPoint(entryPoint))
                .authorizeExchange(ex -> ex
                        .pathMatchers("/actuator/health/**", "/actuator/info", "/actuator/prometheus").permitAll()
                        .pathMatchers("/api/orders/**").hasAnyRole("ADMIN", "TRADER", "READ_ONLY")
                        .anyExchange().authenticated()
                )
                .addFilterAt(jwtFilter, SecurityWebFiltersOrder.AUTHENTICATION);
        return http.build();
    }
}
