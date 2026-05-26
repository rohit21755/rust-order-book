package com.hft.marketdata.ticker;

import com.hft.marketdata.model.Ticker;
import com.hft.marketdata.model.TradeMessage;
import com.hft.marketdata.redis.TickerCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TickerServiceTest {

    TickerCache cache;
    TickerService svc;

    @BeforeEach
    void setUp() {
        cache = mock(TickerCache.class);
        when(cache.put(any(Ticker.class))).thenReturn(Mono.just(true));
        when(cache.get(anyString())).thenReturn(Mono.empty());
        svc = new TickerService(cache);
    }

    private TradeMessage trade(String price, String qty, long ts) {
        return new TradeMessage("t", "b", "s", "bu", "se", "BTC-USDT",
                new BigDecimal(price), new BigDecimal(qty), ts, 1);
    }

    @Test
    void computesHighLowVolumeAndChange() {
        long now = System.currentTimeMillis();
        svc.onTrade(trade("100", "1", now - 3000)).block();
        svc.onTrade(trade("110", "2", now - 2000)).block();
        svc.onTrade(trade("90", "1", now - 1000)).block();

        StepVerifier.create(svc.current("BTC-USDT"))
                .assertNext(t -> {
                    assert t.high24h().compareTo(new BigDecimal("110")) == 0;
                    assert t.low24h().compareTo(new BigDecimal("90")) == 0;
                    assert t.volume24h().compareTo(new BigDecimal("4")) == 0;
                    assert t.lastPrice().compareTo(new BigDecimal("90")) == 0;
                    // change = last(90) - first(100) = -10
                    assert t.priceChange24h().compareTo(new BigDecimal("-10")) == 0;
                })
                .verifyComplete();
    }

    @Test
    void unknownSymbolReturnsZeroTicker() {
        StepVerifier.create(svc.current("NONE"))
                .assertNext(t -> {
                    assert t.symbol().equals("NONE");
                    assert t.lastPrice().signum() == 0;
                })
                .verifyComplete();
    }
}
