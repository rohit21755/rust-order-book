package com.hft.risk.controller;

import com.hft.risk.config.RiskParametersHolder;
import com.hft.risk.config.RiskProperties;
import com.hft.risk.redis.ActivityCounter;
import com.hft.risk.redis.HaltRegistry;
import com.hft.risk.service.BalanceClient;
import com.hft.risk.service.DailyPnlClient;
import com.hft.shared.security.AuthenticatedUserPrincipal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RiskControllerTest {

    RiskParametersHolder holder;
    HaltRegistry halt;
    BalanceClient balance;
    DailyPnlClient pnl;
    ActivityCounter activity;
    WebTestClient client;
    UUID admin = UUID.randomUUID();

    @BeforeEach
    void setUp() {
        RiskProperties props = new RiskProperties();
        holder = new RiskParametersHolder(props);
        holder.update(new RiskParametersHolder.RiskParameters(
                new BigDecimal("10"), new BigDecimal("100000"), new BigDecimal("50000"),
                new BigDecimal("-10000"), 100, 60, new BigDecimal("0.05")));
        halt = mock(HaltRegistry.class);
        balance = mock(BalanceClient.class);
        pnl = mock(DailyPnlClient.class);
        activity = mock(ActivityCounter.class);

        client = WebTestClient.bindToController(new RiskController(holder, halt, balance, pnl, activity))
                .build()
                .mutateWith(SecurityMockServerConfigurers.mockAuthentication(
                        new AuthenticatedUserPrincipal(admin, "t",
                                List.of(new SimpleGrantedAuthority("ROLE_ADMIN")))));
    }

    @Test
    void getConfigReturnsCurrent() {
        client.get().uri("/api/risk/config")
                .exchange().expectStatus().isOk()
                .expectBody()
                .jsonPath("$.maxOrderValue").isEqualTo(50000);
    }

    @Test
    void haltMarksUser() {
        UUID uid = UUID.randomUUID();
        when(halt.halt(uid)).thenReturn(Mono.just(true));

        client.post().uri("/api/risk/users/{uid}/halt", uid)
                .exchange().expectStatus().isOk()
                .expectBody().jsonPath("$").isEqualTo(true);
    }

    @Test
    void statusReturnsAggregatedFields() {
        UUID uid = UUID.randomUUID();
        when(halt.isHalted(uid)).thenReturn(Mono.just(false));
        when(balance.balance(uid)).thenReturn(Mono.just(new BigDecimal("10000")));
        when(pnl.dailyPnl(uid)).thenReturn(Mono.just(new BigDecimal("-500")));
        when(activity.count(any(), anyInt())).thenReturn(Mono.just(7L));

        client.get().uri("/api/risk/users/{uid}/status", uid)
                .exchange().expectStatus().isOk()
                .expectBody()
                .jsonPath("$.halted").isEqualTo(false)
                .jsonPath("$.balance").isEqualTo(10000)
                .jsonPath("$.recentOrderCount").isEqualTo(7);
    }
}
