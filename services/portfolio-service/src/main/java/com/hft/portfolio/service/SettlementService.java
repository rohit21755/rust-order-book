package com.hft.portfolio.service;

import com.hft.kafka.event.PortfolioEvent;
import com.hft.kafka.event.EventEnvelope;
import com.hft.kafka.producer.KafkaEventPublisher;
import com.hft.portfolio.config.PortfolioProperties;
import com.hft.portfolio.domain.Holding;
import com.hft.portfolio.domain.PnlRecord;
import com.hft.portfolio.domain.ProcessedTrade;
import com.hft.portfolio.domain.UserTrade;
import com.hft.portfolio.kafka.TradeMessage;
import com.hft.portfolio.redis.PortfolioCache;
import com.hft.portfolio.repository.HoldingRepository;
import com.hft.portfolio.repository.PnlRecordRepository;
import com.hft.portfolio.repository.ProcessedTradeRepository;
import com.hft.portfolio.repository.UserTradeRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

/**
 * Settles trades into holdings + PnL. Exactly-once via processed_trades insert; the whole
 * settlement runs in a single reactive transaction. PortfolioEvents are published AFTER commit.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SettlementService {

    private final HoldingRepository holdingRepo;
    private final PnlRecordRepository pnlRepo;
    private final ProcessedTradeRepository processedRepo;
    private final UserTradeRepository userTradeRepo;
    private final PnlCalculator calc;
    private final PortfolioCache cache;
    private final KafkaEventPublisher<EventEnvelope> publisher;
    private final TransactionalOperator txOperator;
    private final PortfolioProperties props;

    private record SettleResult(UUID userId, Holding holding, BigDecimal realized) {}

    /**
     * Idempotent settlement. Duplicate tradeId → no-op (returns empty).
     */
    public Mono<Void> settle(TradeMessage trade) {
        OffsetDateTime ts = OffsetDateTime.ofInstant(Instant.ofEpochMilli(trade.executedAtMs()), ZoneOffset.UTC);
        UUID buyer = UUID.fromString(trade.buyerUserId());
        UUID seller = UUID.fromString(trade.sellerUserId());

        // DB work runs in ONE transaction, sequentially (single R2DBC connection).
        Mono<reactor.util.function.Tuple2<SettleResult, SettleResult>> dbWork =
                processedRepo.save(ProcessedTrade.builder()
                                .tradeId(trade.tradeId()).processedAt(OffsetDateTime.now()).build())
                        .then(settleBuy(buyer, trade, ts))
                        .flatMap(buy -> settleSell(seller, trade, ts)
                                .map(sell -> reactor.util.function.Tuples.of(buy, sell)));

        return dbWork.as(txOperator::transactional)
                // Optimistic conflicts → retry whole txn (consumer is serialized, so rare).
                .retryWhen(Retry.backoff(props.getSettlement().getOptimisticRetryMax(), Duration.ofMillis(20))
                        .filter(SettlementService::isRetryable))
                // Publish PortfolioEvents AFTER commit (avoids duplicate events on rollback/retry).
                .flatMap(tuple -> publishEvents(tuple.getT1(), tuple.getT2(), ts)
                        .then(cache.invalidate(buyer))
                        .then(cache.invalidate(seller)))
                // Duplicate trade → already settled; swallow.
                .onErrorResume(SettlementService::isDuplicate, e -> {
                    log.debug("trade {} already settled; skipping", trade.tradeId());
                    return Mono.empty();
                })
                .then();
    }

    private Mono<SettleResult> settleBuy(UUID buyer, TradeMessage t, OffsetDateTime ts) {
        return holdingRepo.findByUserIdAndSymbol(buyer, t.symbol())
                .defaultIfEmpty(newHolding(buyer, t.symbol()))
                .flatMap(h -> {
                    BigDecimal oldQty = h.getQuantity();
                    BigDecimal newAvg = calc.weightedAvg(oldQty, h.getAvgBuyPrice(), t.quantity(), t.price());
                    h.setAvgBuyPrice(newAvg);
                    h.setQuantity(oldQty.add(t.quantity()));
                    h.setUpdatedAt(OffsetDateTime.now());
                    return holdingRepo.save(h)
                            .flatMap(saved -> recordTrade(buyer, t, "BUY", BigDecimal.ZERO, ts)
                                    .then(appendPnl(buyer, t.symbol(), BigDecimal.ZERO, ts))
                                    .thenReturn(new SettleResult(buyer, saved, BigDecimal.ZERO)));
                });
    }

    private Mono<SettleResult> settleSell(UUID seller, TradeMessage t, OffsetDateTime ts) {
        return holdingRepo.findByUserIdAndSymbol(seller, t.symbol())
                .flatMap(h -> {
                    BigDecimal oldQty = h.getQuantity();
                    // Validate: never go negative — sell only what is held.
                    BigDecimal sellable = oldQty.min(t.quantity());
                    if (sellable.signum() <= 0) {
                        log.warn("seller {} has no {} position; skipping sell settlement", seller, t.symbol());
                        return Mono.just(new SettleResult(seller, h, BigDecimal.ZERO));
                    }
                    BigDecimal realized = calc.realizedOnSell(t.price(), h.getAvgBuyPrice(), sellable);
                    BigDecimal newQty = oldQty.subtract(sellable);
                    h.setQuantity(newQty);
                    if (newQty.signum() == 0) {
                        h.setAvgBuyPrice(BigDecimal.ZERO);
                    }
                    h.setUpdatedAt(OffsetDateTime.now());
                    return holdingRepo.save(h)
                            .flatMap(saved -> recordTrade(seller, t, "SELL", realized, ts)
                                    .then(appendPnl(seller, t.symbol(), realized, ts))
                                    .thenReturn(new SettleResult(seller, saved, realized)));
                })
                .switchIfEmpty(Mono.defer(() -> {
                    log.warn("seller {} unknown holding {}; recording zero-position sell", seller, t.symbol());
                    return recordTrade(seller, t, "SELL", BigDecimal.ZERO, ts)
                            .thenReturn(new SettleResult(seller, newHolding(seller, t.symbol()), BigDecimal.ZERO));
                }));
    }

    private Holding newHolding(UUID userId, String symbol) {
        return Holding.builder()
                .userId(userId).symbol(symbol)
                .quantity(BigDecimal.ZERO).avgBuyPrice(BigDecimal.ZERO)
                .updatedAt(OffsetDateTime.now())
                .build();
    }

    private Mono<UserTrade> recordTrade(UUID userId, TradeMessage t, String side, BigDecimal realized, OffsetDateTime ts) {
        return userTradeRepo.save(UserTrade.builder()
                .userId(userId).tradeId(t.tradeId()).symbol(t.symbol())
                .side(side).price(t.price()).quantity(t.quantity())
                .realizedPnl(realized).executedAt(ts)
                .build());
    }

    private Mono<PnlRecord> appendPnl(UUID userId, String symbol, BigDecimal realized, OffsetDateTime ts) {
        return pnlRepo.save(PnlRecord.builder()
                .userId(userId).symbol(symbol)
                .realizedPnl(realized).unrealizedPnl(BigDecimal.ZERO)
                .timestamp(ts)
                .build());
    }

    private Mono<Void> publishEvents(SettleResult buy, SettleResult sell, OffsetDateTime ts) {
        Flux<PortfolioEvent> events = Flux.just(
                toEvent(buy, ts), toEvent(sell, ts));
        return events.flatMap(ev -> publisher.publish(props.getKafka().getPortfolioEventsTopic(), ev)
                        .onErrorResume(e -> {
                            log.error("portfolio event publish failed for {}: {}", ev.userId(), e.toString());
                            return Mono.empty();
                        }))
                .then();
    }

    private PortfolioEvent toEvent(SettleResult r, OffsetDateTime ts) {
        Holding h = r.holding();
        return new PortfolioEvent(
                PortfolioEvent.Type.POSITION_UPDATE,
                r.userId(),
                h.getSymbol(),
                h.getQuantity(),
                h.getAvgBuyPrice(),
                r.realized(),
                BigDecimal.ZERO,   // unrealized computed on-the-fly elsewhere
                ts.toInstant());
    }

    private static boolean isRetryable(Throwable t) {
        return t instanceof OptimisticLockingFailureException;
    }

    private static boolean isDuplicate(Throwable t) {
        return t instanceof DuplicateKeyException || t instanceof DataIntegrityViolationException;
    }
}
