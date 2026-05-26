package com.hft.risk.controller;

import com.hft.risk.config.RiskParametersHolder;
import com.hft.risk.config.RiskParametersHolder.RiskParameters;
import com.hft.risk.dto.RiskConfigDto;
import com.hft.risk.dto.UserRiskStatus;
import com.hft.risk.redis.ActivityCounter;
import com.hft.risk.redis.HaltRegistry;
import com.hft.risk.service.BalanceClient;
import com.hft.risk.service.DailyPnlClient;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.UUID;

@RestController
@RequestMapping("/api/risk")
@RequiredArgsConstructor
public class RiskController {

    private final RiskParametersHolder holder;
    private final HaltRegistry haltRegistry;
    private final BalanceClient balanceClient;
    private final DailyPnlClient pnlClient;
    private final ActivityCounter activity;

    @GetMapping("/config")
    public Mono<RiskParameters> getConfig() {
        return Mono.just(holder.get());
    }

    @PutMapping("/config")
    @PreAuthorize("hasRole('ADMIN')")
    @ResponseStatus(HttpStatus.OK)
    public Mono<RiskParameters> updateConfig(@Valid @RequestBody RiskConfigDto body) {
        RiskParameters next = new RiskParameters(
                body.maxLeverage(), body.maxPositionSize(), body.maxOrderValue(),
                body.maxDailyLoss(), body.abnormalActivityThreshold(),
                body.abnormalActivityWindowSeconds(), body.priceDeviationPct());
        holder.update(next);
        return Mono.just(next);
    }

    @GetMapping("/users/{userId}/status")
    public Mono<UserRiskStatus> status(@PathVariable UUID userId) {
        int window = holder.get().abnormalActivityWindowSeconds();
        return Mono.zip(
                        haltRegistry.isHalted(userId).defaultIfEmpty(false),
                        balanceClient.balance(userId).defaultIfEmpty(java.math.BigDecimal.ZERO),
                        pnlClient.dailyPnl(userId).defaultIfEmpty(java.math.BigDecimal.ZERO),
                        activity.count(userId, window).defaultIfEmpty(0L))
                .map(t -> new UserRiskStatus(userId, t.getT1(), t.getT2(), t.getT3(), t.getT4()));
    }

    @PostMapping("/users/{userId}/halt")
    @PreAuthorize("hasRole('ADMIN')")
    public Mono<Boolean> halt(@PathVariable UUID userId) {
        return haltRegistry.halt(userId);
    }

    @PostMapping("/users/{userId}/resume")
    @PreAuthorize("hasRole('ADMIN')")
    public Mono<Boolean> resume(@PathVariable UUID userId) {
        return haltRegistry.resume(userId);
    }
}
