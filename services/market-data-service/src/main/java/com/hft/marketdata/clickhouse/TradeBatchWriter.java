package com.hft.marketdata.clickhouse;

import com.hft.marketdata.config.MarketDataProperties;
import com.hft.marketdata.model.TradeMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Batches raw trades to ClickHouse: flush when ≥ batchMaxRecords OR every batchMaxIntervalMs.
 * Satisfies the "all ClickHouse writes batched (min 100 or every 1s)" constraint.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TradeBatchWriter {

    private final ClickHouseGateway gateway;
    private final MarketDataProperties props;

    private final ConcurrentLinkedQueue<TradeMessage> queue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger size = new AtomicInteger();

    /** Enqueue a trade; trigger an immediate flush if the size threshold is reached. */
    public void enqueue(TradeMessage trade) {
        queue.add(trade);
        if (size.incrementAndGet() >= props.getClickhouse().getBatchMaxRecords()) {
            flush();
        }
    }

    /** Time-based flush. */
    @Scheduled(fixedDelayString = "${market.clickhouse.batch-max-interval-ms:1000}")
    public void scheduledFlush() {
        flush();
    }

    private synchronized void flush() {
        if (queue.isEmpty()) return;
        List<TradeMessage> batch = new ArrayList<>();
        TradeMessage t;
        while ((t = queue.poll()) != null) {
            batch.add(t);
            size.decrementAndGet();
        }
        if (batch.isEmpty()) return;
        gateway.insertTrades(batch)
                .subscribe(
                        n -> log.debug("flushed {} trades to ClickHouse", n),
                        err -> {
                            log.error("ClickHouse trade flush failed; re-queueing {} rows", batch.size(), err);
                            queue.addAll(batch);
                            size.addAndGet(batch.size());
                        });
    }
}
