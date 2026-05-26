package com.hft.portfolio.controller;

import com.hft.portfolio.dto.HoldingDto;
import com.hft.portfolio.dto.PnlSummary;
import com.hft.portfolio.service.PortfolioService;
import com.hft.shared.security.AuthenticatedUserPrincipal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PortfolioControllerTest {

    PortfolioService service;
    UUID userId;
    WebTestClient client;

    @BeforeEach
    void setUp() {
        service = mock(PortfolioService.class);
        userId = UUID.randomUUID();
        var principal = new AuthenticatedUserPrincipal(userId, "t",
                List.of(new SimpleGrantedAuthority("ROLE_TRADER")));
        client = WebTestClient.bindToController(new PortfolioController(service))
                .build()
                .mutateWith(SecurityMockServerConfigurers.mockAuthentication(principal));
    }

    @Test
    void holdingsReturnsEnrichedList() {
        when(service.holdings(any())).thenReturn(Flux.just(
                new HoldingDto("BTC-USDT", new BigDecimal("2"), new BigDecimal("100"),
                        new BigDecimal("110"), new BigDecimal("220"), new BigDecimal("20"))));

        client.get().uri("/api/portfolio/holdings")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$[0].symbol").isEqualTo("BTC-USDT")
                .jsonPath("$[0].unrealizedPnl").isEqualTo("20");
    }

    @Test
    void pnlReturnsSummary() {
        when(service.pnlSummary(any())).thenReturn(Mono.just(
                new PnlSummary(new BigDecimal("40"), new BigDecimal("20"), new BigDecimal("60"))));

        client.get().uri("/api/portfolio/pnl")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.realizedPnl").isEqualTo("40")
                .jsonPath("$.totalPnl").isEqualTo("60");
    }
}
