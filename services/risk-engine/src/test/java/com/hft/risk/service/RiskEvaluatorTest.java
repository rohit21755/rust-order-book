package com.hft.risk.service;

import com.hft.kafka.event.EventEnvelope;
import com.hft.kafka.producer.KafkaEventPublisher;
import com.hft.risk.checks.AbnormalActivityCheck;
import com.hft.risk.checks.DailyLossCheck;
import com.hft.risk.checks.ExposureCheck;
import com.hft.risk.checks.LeverageCheck;
import com.hft.risk.checks.MaxOrderValueCheck;
import com.hft.risk.checks.PriceDeviationCheck;
import com.hft.risk.config.RiskParametersHolder;
import com.hft.risk.config.RiskProperties;
import com.hft.risk.kafka.OrderMessage;
import com.hft.risk.metrics.RiskMetrics;
import com.hft.risk.redis.ActivityCounter;
import com.hft.risk.redis.HaltRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RiskEvaluatorTest {

    RiskParametersHolder holder;
    BalanceClient balance;
    MarketPriceClient price;
    DailyPnlClient pnl;
    ActivityCounter activity;
    HaltRegistry halt;
    @SuppressWarnings("unchecked")
    KafkaEventPublisher<EventEnvelope> publisher = mock(KafkaEventPublisher.class);
    RiskMetrics metrics;
    RiskEvaluator evaluator;

    @BeforeEach
    void setUp() {
        RiskProperties props = new RiskProperties();
        holder = new RiskParametersHolder(props);
        // emulate @PostConstruct
        holder.update(new RiskParametersHolder.RiskParameters(
                new BigDecimal("10"), new BigDecimal("100000"), new BigDecimal("50000"),
                new BigDecimal("-10000"), 100, 60, new BigDecimal("0.05")));

        balance = mock(BalanceClient.class);
        price = mock(MarketPriceClient.class);
        pnl = mock(DailyPnlClient.class);
        activity = mock(ActivityCounter.class);
        halt = mock(HaltRegistry.class);
        metrics = new RiskMetrics(new SimpleMeterRegistry());

        when(publisher.publish(anyString(), any())).thenReturn(Mono.empty());

        evaluator = new RiskEvaluator(
                List.of(new MaxOrderValueCheck(), new LeverageCheck(), new ExposureCheck(),
                        new AbnormalActivityCheck(), new DailyLossCheck(), new PriceDeviationCheck()),
                holder, balance, price, pnl, activity, halt, publisher, props, metrics);
    }

    private OrderMessage order(String type, String px, String qty) {
        return new OrderMessage("NEW", UUID.randomUUID(), UUID.randomUUID(),
                "BTC-USDT", "BUY", type,
                px == null ? null : new BigDecimal(px), null, new BigDecimal(qty), "k");
    }

    @Test
    void approvesValidOrder() {
        when(halt.isHalted(any())).thenReturn(Mono.just(false));
        when(balance.balance(any())).thenReturn(Mono.just(new BigDecimal("100000")));
        when(price.lastPrice(anyString())).thenReturn(Mono.just(new BigDecimal("100")));
        when(pnl.dailyPnl(any())).thenReturn(Mono.just(new BigDecimal("0")));
        when(activity.recordAndCount(any(), anyInt())).thenReturn(Mono.just(1L));

        StepVerifier.create(evaluator.evaluate(order("LIMIT", "100", "10")))
                .assertNext(d -> {
                    assert d.approved();
                })
                .verifyComplete();
    }

    @Test
    void rejectsHaltedUser() {
        when(halt.isHalted(any())).thenReturn(Mono.just(true));
        when(balance.balance(any())).thenReturn(Mono.just(new BigDecimal("100000")));
        when(price.lastPrice(anyString())).thenReturn(Mono.just(new BigDecimal("100")));
        when(pnl.dailyPnl(any())).thenReturn(Mono.just(new BigDecimal("0")));
        when(activity.recordAndCount(any(), anyInt())).thenReturn(Mono.just(1L));

        StepVerifier.create(evaluator.evaluate(order("LIMIT", "100", "10")))
                .assertNext(d -> {
                    assert !d.approved();
                    assert d.reasons().get(0).equals("USER_HALTED");
                })
                .verifyComplete();
    }

    @Test
    void rejectsExcessiveOrderValue() {
        when(halt.isHalted(any())).thenReturn(Mono.just(false));
        when(balance.balance(any())).thenReturn(Mono.just(new BigDecimal("1000000")));
        when(price.lastPrice(anyString())).thenReturn(Mono.just(new BigDecimal("100")));
        when(pnl.dailyPnl(any())).thenReturn(Mono.just(new BigDecimal("0")));
        when(activity.recordAndCount(any(), anyInt())).thenReturn(Mono.just(1L));

        // 100 * 600 = 60000 > 50000
        StepVerifier.create(evaluator.evaluate(order("LIMIT", "100", "600")))
                .assertNext(d -> {
                    assert !d.approved();
                    assert d.reasons().stream().anyMatch(r -> r.startsWith("MAX_ORDER_VALUE"));
                })
                .verifyComplete();
    }

    @Test
    void cancelOrderBypassesChecks() {
        OrderMessage cancel = new OrderMessage("CANCEL", UUID.randomUUID(), UUID.randomUUID(),
                "BTC-USDT", "BUY", "LIMIT", null, null, new BigDecimal("1"), "k");

        StepVerifier.create(evaluator.evaluate(cancel))
                .assertNext(d -> { assert d.approved(); })
                .verifyComplete();
    }
}
