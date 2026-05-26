package com.hft.marketdata.fanout;

import com.hft.marketdata.model.StreamType;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

class MarketDataFanoutTest {

    @Test
    void fanOutDeliversToMultipleSubscribers() {
        MarketDataFanout fanout = new MarketDataFanout();
        Flux<Object> a = fanout.stream(StreamType.TRADES, "BTC-USDT");
        Flux<Object> b = fanout.stream(StreamType.TRADES, "BTC-USDT");

        StepVerifier.create(Flux.merge(a.take(1), b.take(1)).collectList()
                        .doOnSubscribe(s -> {
                            // emit after subscription on a separate thread tick
                            new Thread(() -> {
                                try { Thread.sleep(50); } catch (InterruptedException ignored) {}
                                fanout.emit(StreamType.TRADES, "BTC-USDT", "tick");
                            }).start();
                        }))
                .assertNext(list -> assertThat(list).containsExactly("tick", "tick"))
                .verifyComplete();
    }

    @Test
    void emitWithNoSubscribersDoesNotThrow() {
        MarketDataFanout fanout = new MarketDataFanout();
        fanout.emit(StreamType.ORDERBOOK, "ETH-USDT", "x"); // no subscriber → best-effort drop
        assertThat(fanout.sinkCount()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void backpressureBufferDropsOldestKeepsNewest() {
        // Drop-oldest buffer of size 2 fed 5 items, requesting 2 → newest survive.
        Flux<Integer> source = Flux.range(1, 5)
                .onBackpressureBuffer(2, dropped -> {}, BufferOverflowStrategy.DROP_OLDEST);

        StepVerifier.create(source, 0)
                .thenRequest(2)
                .expectNext(4, 5)
                .thenCancel()
                .verify();
    }
}
