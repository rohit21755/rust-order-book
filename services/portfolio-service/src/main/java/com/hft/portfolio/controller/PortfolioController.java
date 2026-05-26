package com.hft.portfolio.controller;

import com.hft.portfolio.dto.HoldingDto;
import com.hft.portfolio.dto.PnlHistoryPoint;
import com.hft.portfolio.dto.PnlSummary;
import com.hft.portfolio.dto.PortfolioDto;
import com.hft.portfolio.dto.TradeDto;
import com.hft.portfolio.service.PortfolioService;
import com.hft.shared.security.AuthenticatedUserPrincipal;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.util.UUID;

@RestController
@RequestMapping("/api/portfolio")
@RequiredArgsConstructor
public class PortfolioController {

    private final PortfolioService service;

    @GetMapping
    public Mono<PortfolioDto> portfolio() {
        return currentUserId().flatMap(service::portfolio);
    }

    @GetMapping("/holdings")
    public Flux<HoldingDto> holdings() {
        return currentUserId().flatMapMany(service::holdings);
    }

    @GetMapping("/pnl")
    public Mono<PnlSummary> pnl() {
        return currentUserId().flatMap(service::pnlSummary);
    }

    @GetMapping("/pnl/history")
    public Flux<PnlHistoryPoint> pnlHistory(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime to) {
        return currentUserId().flatMapMany(uid -> service.pnlHistory(uid, from, to));
    }

    @GetMapping("/trades")
    public Flux<TradeDto> trades(
            @RequestParam(defaultValue = "50") int limit,
            @RequestParam(defaultValue = "0") int offset) {
        return currentUserId().flatMapMany(uid -> service.trades(uid, limit, offset));
    }

    private Mono<UUID> currentUserId() {
        return ReactiveSecurityContextHolder.getContext()
                .map(ctx -> (AuthenticatedUserPrincipal) ctx.getAuthentication())
                .map(AuthenticatedUserPrincipal::getUserId);
    }
}
