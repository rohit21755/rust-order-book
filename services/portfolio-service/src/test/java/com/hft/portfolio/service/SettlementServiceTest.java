package com.hft.portfolio.service;

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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class SettlementServiceTest {

    HoldingRepository holdingRepo;
    PnlRecordRepository pnlRepo;
    ProcessedTradeRepository processedRepo;
    UserTradeRepository userTradeRepo;
    PortfolioCache cache;
    KafkaEventPublisher<EventEnvelope> publisher;
    TransactionalOperator tx;
    SettlementService svc;

    UUID buyer = UUID.randomUUID();
    UUID seller = UUID.randomUUID();

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        holdingRepo = mock(HoldingRepository.class);
        pnlRepo = mock(PnlRecordRepository.class);
        processedRepo = mock(ProcessedTradeRepository.class);
        userTradeRepo = mock(UserTradeRepository.class);
        cache = mock(PortfolioCache.class);
        publisher = mock(KafkaEventPublisher.class);
        tx = mock(TransactionalOperator.class);
        PortfolioProperties props = new PortfolioProperties();

        svc = new SettlementService(holdingRepo, pnlRepo, processedRepo, userTradeRepo,
                new PnlCalculator(), cache, publisher, tx, props);

        // tx passthrough
        when(tx.transactional(any(Mono.class))).thenAnswer(inv -> inv.getArgument(0));
        when(cache.invalidate(any())).thenReturn(Mono.just(1L));
        when(processedRepo.save(any(ProcessedTrade.class))).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        when(holdingRepo.save(any(Holding.class))).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        when(pnlRepo.save(any(PnlRecord.class))).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        when(userTradeRepo.save(any(UserTrade.class))).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        when(publisher.publish(anyString(), any())).thenReturn(Mono.empty());
    }

    private TradeMessage trade(String price, String qty) {
        return new TradeMessage("trade-1", "bo", "so", buyer.toString(), seller.toString(),
                "BTC-USDT", new BigDecimal(price), new BigDecimal(qty), System.currentTimeMillis(), 1);
    }

    @Test
    void settlesBuyerAndSellerComputesRealized() {
        // buyer has no holding; seller holds 5 @ 100
        when(holdingRepo.findByUserIdAndSymbol(eq(buyer), anyString())).thenReturn(Mono.empty());
        when(holdingRepo.findByUserIdAndSymbol(eq(seller), anyString())).thenReturn(Mono.just(
                Holding.builder().id(UUID.randomUUID()).userId(seller).symbol("BTC-USDT")
                        .quantity(new BigDecimal("5")).avgBuyPrice(new BigDecimal("100")).version(0L).build()));

        StepVerifier.create(svc.settle(trade("120", "2"))).verifyComplete();

        ArgumentCaptor<UserTrade> cap = ArgumentCaptor.forClass(UserTrade.class);
        verify(userTradeRepo, times(2)).save(cap.capture());
        UserTrade sell = cap.getAllValues().stream().filter(u -> u.getSide().equals("SELL")).findFirst().orElseThrow();
        // realized = (120-100)*2 = 40
        assertThat(sell.getRealizedPnl()).isEqualByComparingTo("40");

        verify(publisher, times(2)).publish(anyString(), any());
        verify(cache).invalidate(buyer);
        verify(cache).invalidate(seller);
    }

    @Test
    void duplicateTradeIsSkipped() {
        when(processedRepo.save(any(ProcessedTrade.class)))
                .thenReturn(Mono.error(new DuplicateKeyException("dup")));

        StepVerifier.create(svc.settle(trade("120", "2"))).verifyComplete();

        verify(holdingRepo, never()).save(any());
    }

    @Test
    void sellerWithoutEnoughPositionNeverGoesNegative() {
        when(holdingRepo.findByUserIdAndSymbol(eq(buyer), anyString())).thenReturn(Mono.empty());
        // seller holds only 1, trade sells 5
        when(holdingRepo.findByUserIdAndSymbol(eq(seller), anyString())).thenReturn(Mono.just(
                Holding.builder().id(UUID.randomUUID()).userId(seller).symbol("BTC-USDT")
                        .quantity(new BigDecimal("1")).avgBuyPrice(new BigDecimal("100")).version(0L).build()));

        StepVerifier.create(svc.settle(trade("120", "5"))).verifyComplete();

        ArgumentCaptor<Holding> cap = ArgumentCaptor.forClass(Holding.class);
        verify(holdingRepo, times(2)).save(cap.capture());
        Holding sellerHolding = cap.getAllValues().stream()
                .filter(h -> h.getUserId().equals(seller)).findFirst().orElseThrow();
        assertThat(sellerHolding.getQuantity().signum()).isGreaterThanOrEqualTo(0);
        assertThat(sellerHolding.getQuantity()).isEqualByComparingTo("0");
    }
}
