package com.hft.marketdata.candle;

import com.hft.marketdata.clickhouse.ClickHouseGateway;
import com.hft.marketdata.model.Candle;
import com.hft.marketdata.model.TradeMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CandleAggregatorTest {

    ClickHouseGateway gateway;
    CandleAggregator agg;

    @BeforeEach
    void setUp() {
        gateway = mock(ClickHouseGateway.class);
        when(gateway.insertCandles(anyList())).thenReturn(Mono.just(0));
        agg = new CandleAggregator(gateway);
    }

    private TradeMessage trade(String price, String qty, long ts) {
        return new TradeMessage("t", "b", "s", "bu", "se", "BTC-USDT",
                new BigDecimal(price), new BigDecimal(qty), ts, 1);
    }

    @Test
    void aggregatesIntoAllIntervals() {
        long t0 = 1_700_000_000_000L; // fixed
        agg.onTrade(trade("100", "1", t0));
        agg.onTrade(trade("105", "2", t0 + 1000));
        agg.onTrade(trade("95", "1", t0 + 2000));
        // 5 interval buckets created for one symbol (1m,5m,15m,1h,1d).
        assertThat(agg.liveBucketCount()).isEqualTo(5);
    }

    @Test
    void sealFlushesClosedBucketsAndComputesOhlcv() {
        long old = System.currentTimeMillis() - (2 * 86_400_000L); // 2 days ago → all intervals closed
        agg.onTrade(trade("100", "1", old));
        agg.onTrade(trade("120", "2", old + 1000));
        agg.onTrade(trade("80", "3", old + 2000));

        List<Candle>[] captured = new List[1];
        when(gateway.insertCandles(anyList())).thenAnswer(inv -> {
            captured[0] = inv.getArgument(0);
            return Mono.just(((List<?>) inv.getArgument(0)).size());
        });

        agg.sealAndFlush();

        assertThat(captured[0]).isNotNull();
        Candle oneMin = captured[0].stream().filter(c -> c.interval().equals("1m")).findFirst().orElseThrow();
        assertThat(oneMin.open()).isEqualByComparingTo("100");
        assertThat(oneMin.high()).isEqualByComparingTo("120");
        assertThat(oneMin.low()).isEqualByComparingTo("80");
        assertThat(oneMin.close()).isEqualByComparingTo("80");
        assertThat(oneMin.volume()).isEqualByComparingTo("6");
        assertThat(oneMin.tradeCount()).isEqualTo(3);
        assertThat(agg.liveBucketCount()).isZero();
    }
}
