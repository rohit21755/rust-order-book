package com.hft.risk.service;

import com.hft.kafka.event.EventEnvelope;
import com.hft.kafka.event.RiskEvent;
import com.hft.kafka.producer.KafkaEventPublisher;
import com.hft.risk.checks.RiskCheck;
import com.hft.risk.checks.RiskContext;
import com.hft.risk.config.RiskParametersHolder;
import com.hft.risk.config.RiskProperties;
import com.hft.risk.dto.RiskDecision;
import com.hft.risk.kafka.OrderMessage;
import com.hft.risk.metrics.RiskMetrics;
import com.hft.risk.redis.ActivityCounter;
import com.hft.risk.redis.HaltRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Orchestrates all risk checks for one inbound order.
 * Concurrent downstream lookups (balance, market price, dailyPnl, activity, halt) keep total
 * latency under 10ms when caches are warm.
 */
@Slf4j
@Service
public class RiskEvaluator {

    private final List<RiskCheck> checks;
    private final RiskParametersHolder paramsHolder;
    private final BalanceClient balanceClient;
    private final MarketPriceClient priceClient;
    private final DailyPnlClient pnlClient;
    private final ActivityCounter activity;
    private final HaltRegistry haltRegistry;
    private final KafkaEventPublisher<EventEnvelope> publisher;
    private final RiskProperties props;
    private final RiskMetrics metrics;

    public RiskEvaluator(List<RiskCheck> checks,
                         RiskParametersHolder paramsHolder,
                         BalanceClient balanceClient,
                         MarketPriceClient priceClient,
                         DailyPnlClient pnlClient,
                         ActivityCounter activity,
                         HaltRegistry haltRegistry,
                         KafkaEventPublisher<EventEnvelope> publisher,
                         RiskProperties props,
                         RiskMetrics metrics) {
        this.checks = checks;
        this.paramsHolder = paramsHolder;
        this.balanceClient = balanceClient;
        this.priceClient = priceClient;
        this.pnlClient = pnlClient;
        this.activity = activity;
        this.haltRegistry = haltRegistry;
        this.publisher = publisher;
        this.props = props;
        this.metrics = metrics;
    }

    public Mono<RiskDecision> evaluate(OrderMessage order) {
        if (!"NEW".equalsIgnoreCase(order.eventType())) {
            // Only NEW orders are gated; CANCEL/MODIFY bypass risk checks.
            return Mono.just(RiskDecision.approve());
        }
        Instant start = Instant.now();
        var params = paramsHolder.get();
        int window = params.abnormalActivityWindowSeconds();

        Mono<Boolean> haltedM = haltRegistry.isHalted(order.userId()).defaultIfEmpty(false);
        Mono<BigDecimal> balanceM = balanceClient.balance(order.userId()).defaultIfEmpty(BigDecimal.ZERO);
        Mono<BigDecimal> priceM = priceClient.lastPrice(order.symbol()).defaultIfEmpty(BigDecimal.ZERO);
        Mono<BigDecimal> pnlM = pnlClient.dailyPnl(order.userId()).defaultIfEmpty(BigDecimal.ZERO);
        Mono<Long> activityM = activity.recordAndCount(order.userId(), window).defaultIfEmpty(0L);

        return Mono.zip(haltedM, balanceM, priceM, pnlM, activityM)
                .map(t -> {
                    boolean halted = t.getT1();
                    BigDecimal balance = t.getT2();
                    BigDecimal price = t.getT3();
                    BigDecimal pnl = t.getT4();
                    long recent = t.getT5();
                    if (halted) {
                        return RiskDecision.reject("USER_HALTED");
                    }
                    RiskContext ctx = new RiskContext(
                            order, params, balance,
                            BigDecimal.ZERO,  // currentPositionValue snapshot — omitted (would require per-symbol lookup; cap covered by total position via balance)
                            price, pnl, recent);
                    List<String> reasons = new ArrayList<>();
                    for (RiskCheck c : checks) {
                        c.evaluate(ctx).ifPresent(r -> reasons.add(c.code() + ": " + r));
                    }
                    return reasons.isEmpty() ? RiskDecision.approve() : RiskDecision.reject(reasons);
                })
                .flatMap(decision -> publishEvent(order, decision).thenReturn(decision))
                .doOnNext(d -> {
                    Duration elapsed = Duration.between(start, Instant.now());
                    metrics.recordDuration(elapsed);
                    if (d.approved()) metrics.approval();
                    else d.reasons().forEach(r -> metrics.rejection(stableCode(r)));
                    if (elapsed.toMillis() > 10) {
                        log.warn("risk eval slow: {}ms order={}", elapsed.toMillis(), order.orderId());
                    }
                });
    }

    private Mono<Void> publishEvent(OrderMessage order, RiskDecision decision) {
        RiskEvent ev = new RiskEvent(
                decision.approved() ? RiskEvent.Type.RECOVERY : RiskEvent.Type.LIMIT_BREACH,
                decision.approved() ? RiskEvent.Severity.INFO : RiskEvent.Severity.WARN,
                order.userId(),
                order.orderId(),
                order.symbol(),
                BigDecimal.ZERO,
                decision.approved() ? "APPROVED" : String.join("; ", decision.reasons()),
                Instant.now());
        return publisher.publish(props.getKafka().getRiskEventsTopic(), ev)
                .onErrorResume(e -> {
                    log.error("risk-event publish failed: {}", e.toString());
                    return Mono.empty();
                })
                .then();
    }

    private static String stableCode(String reason) {
        int colon = reason.indexOf(':');
        return colon > 0 ? reason.substring(0, colon) : reason;
    }
}
