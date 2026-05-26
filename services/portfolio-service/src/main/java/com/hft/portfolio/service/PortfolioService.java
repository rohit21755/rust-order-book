package com.hft.portfolio.service;

import com.hft.portfolio.domain.Holding;
import com.hft.portfolio.dto.HoldingDto;
import com.hft.portfolio.dto.PnlHistoryPoint;
import com.hft.portfolio.dto.PnlSummary;
import com.hft.portfolio.dto.PortfolioDto;
import com.hft.portfolio.dto.TradeDto;
import com.hft.portfolio.redis.PortfolioCache;
import com.hft.portfolio.repository.HoldingRepository;
import com.hft.portfolio.repository.PnlRecordRepository;
import com.hft.portfolio.repository.UserTradeRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Read-side. Unrealized PnL + current values computed on-the-fly from live market prices
 * (never stored). Portfolio summary cached in Redis (TTL 10s).
 */
@Service
@RequiredArgsConstructor
public class PortfolioService {

    private final HoldingRepository holdingRepo;
    private final PnlRecordRepository pnlRepo;
    private final UserTradeRepository tradeRepo;
    private final MarketPriceClient priceClient;
    private final PnlCalculator calc;
    private final PortfolioCache cache;

    public Mono<PortfolioDto> portfolio(UUID userId) {
        return cache.get(userId)
                .switchIfEmpty(buildPortfolio(userId).flatMap(cache::put));
    }

    public Flux<HoldingDto> holdings(UUID userId) {
        return holdingRepo.findByUserId(userId).flatMap(this::enrich);
    }

    /** PnL summary: realized = sum of stored realized; unrealized = on-the-fly. */
    public Mono<PnlSummary> pnlSummary(UUID userId) {
        Mono<BigDecimal> realized = pnlRepo.findByUserId(userId)
                .map(r -> r.getRealizedPnl() == null ? BigDecimal.ZERO : r.getRealizedPnl())
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        Mono<BigDecimal> unrealized = holdings(userId)
                .map(HoldingDto::unrealizedPnl)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        return Mono.zip(realized, unrealized)
                .map(t -> new PnlSummary(t.getT1(), t.getT2(), t.getT1().add(t.getT2())));
    }

    public Flux<PnlHistoryPoint> pnlHistory(UUID userId, OffsetDateTime from, OffsetDateTime to) {
        return pnlRepo.history(userId, from, to)
                .map(r -> new PnlHistoryPoint(r.getSymbol(), r.getRealizedPnl(), r.getUnrealizedPnl(), r.getTimestamp()));
    }

    public Flux<TradeDto> trades(UUID userId, int limit, int offset) {
        int boundedLimit = Math.max(1, Math.min(limit, 500));
        int boundedOffset = Math.max(0, offset);
        return tradeRepo.findByUser(userId, boundedLimit, boundedOffset)
                .map(t -> new TradeDto(t.getTradeId(), t.getSymbol(), t.getSide(),
                        t.getPrice(), t.getQuantity(), t.getRealizedPnl(), t.getExecutedAt()));
    }

    private Mono<PortfolioDto> buildPortfolio(UUID userId) {
        return holdings(userId).collectList().flatMap(list -> {
            BigDecimal totalValue = list.stream()
                    .map(HoldingDto::currentValue)
                    .reduce(BigDecimal.ZERO, BigDecimal::add);
            BigDecimal unrealized = list.stream()
                    .map(HoldingDto::unrealizedPnl)
                    .reduce(BigDecimal.ZERO, BigDecimal::add);
            return realizedTotal(userId).map(realized ->
                    new PortfolioDto(userId, list, totalValue,
                            new PnlSummary(realized, unrealized, realized.add(unrealized))));
        });
    }

    private Mono<BigDecimal> realizedTotal(UUID userId) {
        return pnlRepo.findByUserId(userId)
                .map(r -> r.getRealizedPnl() == null ? BigDecimal.ZERO : r.getRealizedPnl())
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private Mono<HoldingDto> enrich(Holding h) {
        return priceClient.lastPrice(h.getSymbol())
                .map(price -> toDto(h, price))
                .switchIfEmpty(Mono.fromSupplier(() -> toDto(h, null)));
    }

    private HoldingDto toDto(Holding h, BigDecimal price) {
        BigDecimal value = calc.value(price, h.getQuantity());
        BigDecimal unreal = calc.unrealized(price, h.getAvgBuyPrice(), h.getQuantity());
        return new HoldingDto(h.getSymbol(), h.getQuantity(), h.getAvgBuyPrice(), price, value, unreal);
    }
}
