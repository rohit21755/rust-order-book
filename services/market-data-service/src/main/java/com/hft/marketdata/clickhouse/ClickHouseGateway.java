package com.hft.marketdata.clickhouse;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.data.ClickHouseFormat;
import com.hft.marketdata.config.MarketDataProperties;
import com.hft.marketdata.model.Candle;
import com.hft.marketdata.model.TradeMessage;
import com.hft.marketdata.model.TradeView;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Thin wrapper over the (blocking) ClickHouse HTTP client. All calls run on
 * {@link Schedulers#boundedElastic()} so the Netty event loop is never blocked.
 */
@Slf4j
@Component
public class ClickHouseGateway {

    private final MarketDataProperties props;
    private final ClickHouseNode node;
    private ClickHouseClient client;

    public ClickHouseGateway(MarketDataProperties props) {
        this.props = props;
        URI uri = URI.create(props.getClickhouse().getUrl());
        this.node = ClickHouseNode.builder()
                .host(uri.getHost())
                .port(ClickHouseProtocol.HTTP, uri.getPort() > 0 ? uri.getPort() : 8123)
                .database(props.getClickhouse().getDatabase())
                .credentials(com.clickhouse.client.ClickHouseCredentials.fromUserAndPassword(
                        props.getClickhouse().getUser(), props.getClickhouse().getPassword()))
                .build();
    }

    @PostConstruct
    void init() {
        this.client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
        log.info("ClickHouse gateway initialised target={}", props.getClickhouse().getUrl());
    }

    @PreDestroy
    void shutdown() {
        if (client != null) client.close();
    }

    /** Batch insert raw trades. */
    public Mono<Integer> insertTrades(List<TradeMessage> trades) {
        if (trades.isEmpty()) return Mono.just(0);
        StringBuilder sql = new StringBuilder(
                "INSERT INTO trades (trade_id,symbol,price,quantity,buy_order_id,sell_order_id,sequence,executed_at) VALUES ");
        for (int i = 0; i < trades.size(); i++) {
            TradeMessage t = trades.get(i);
            if (i > 0) sql.append(',');
            sql.append('(')
               .append(q(t.tradeId())).append(',')
               .append(q(t.symbol())).append(',')
               .append(t.price().toPlainString()).append(',')
               .append(t.quantity().toPlainString()).append(',')
               .append(q(t.buyOrderId())).append(',')
               .append(q(t.sellOrderId())).append(',')
               .append(t.sequence()).append(',')
               .append(q(Instant.ofEpochMilli(t.executedAtMs()).toString()))
               .append(')');
        }
        return execute(sql.toString()).thenReturn(trades.size());
    }

    /** Batch upsert candles (ReplacingMergeTree dedups by ORDER BY key). */
    public Mono<Integer> insertCandles(List<Candle> candles) {
        if (candles.isEmpty()) return Mono.just(0);
        StringBuilder sql = new StringBuilder(
                "INSERT INTO candles (symbol,interval,open_time,open,high,low,close,volume,trade_count) VALUES ");
        for (int i = 0; i < candles.size(); i++) {
            Candle c = candles.get(i);
            if (i > 0) sql.append(',');
            sql.append('(')
               .append(q(c.symbol())).append(',')
               .append(q(c.interval())).append(',')
               .append(q(Instant.ofEpochMilli(c.openTimeMs()).toString())).append(',')
               .append(c.open().toPlainString()).append(',')
               .append(c.high().toPlainString()).append(',')
               .append(c.low().toPlainString()).append(',')
               .append(c.close().toPlainString()).append(',')
               .append(c.volume().toPlainString()).append(',')
               .append(c.tradeCount())
               .append(')');
        }
        return execute(sql.toString()).thenReturn(candles.size());
    }

    /** Query historical candles, newest-first, paginated. */
    public Flux<Candle> queryCandles(String symbol, String interval, int limit, int offset) {
        String sql = "SELECT symbol,interval,toUnixTimestamp64Milli(open_time),open,high,low,close,volume,trade_count "
                + "FROM candles WHERE symbol=" + q(symbol) + " AND interval=" + q(interval)
                + " ORDER BY open_time DESC LIMIT " + limit + " OFFSET " + offset;
        return query(sql, rec -> new Candle(
                rec.get(0), rec.get(1), Long.parseLong(rec.get(2)),
                bd(rec.get(3)), bd(rec.get(4)), bd(rec.get(5)), bd(rec.get(6)), bd(rec.get(7)),
                Integer.parseInt(rec.get(8))));
    }

    /** Recent trades from ClickHouse, newest-first. */
    public Flux<TradeView> queryRecentTrades(String symbol, int limit) {
        String sql = "SELECT trade_id,symbol,price,quantity,sequence,toUnixTimestamp64Milli(executed_at) "
                + "FROM trades WHERE symbol=" + q(symbol) + " ORDER BY executed_at DESC LIMIT " + limit;
        return query(sql, rec -> new TradeView(
                rec.get(0), rec.get(1), bd(rec.get(2)), bd(rec.get(3)),
                Long.parseLong(rec.get(4)), Long.parseLong(rec.get(5))));
    }

    // ---- internals ----

    private Mono<Void> execute(String sql) {
        return Mono.fromCallable(() -> {
            try (ClickHouseResponse resp = client.read(node)
                    .query(sql)
                    .executeAndWait()) {
                return resp.getSummary();
            }
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }

    private <T> Flux<T> query(String sql, java.util.function.Function<List<String>, T> mapper) {
        return Mono.fromCallable(() -> {
            List<T> out = new ArrayList<>();
            try (ClickHouseResponse resp = client.read(node)
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                    .query(sql)
                    .executeAndWait()) {
                resp.records().forEach(record -> {
                    List<String> cols = new ArrayList<>();
                    record.forEach(v -> cols.add(v.asString()));
                    out.add(mapper.apply(cols));
                });
            }
            return out;
        }).subscribeOn(Schedulers.boundedElastic()).flatMapMany(Flux::fromIterable);
    }

    private static String q(String s) {
        return "'" + (s == null ? "" : s.replace("'", "\\'")) + "'";
    }

    private static java.math.BigDecimal bd(String s) {
        return (s == null || s.isEmpty()) ? java.math.BigDecimal.ZERO : new java.math.BigDecimal(s);
    }
}
