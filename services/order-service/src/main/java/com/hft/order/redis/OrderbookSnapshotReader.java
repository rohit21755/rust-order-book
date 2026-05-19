package com.hft.order.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.order.dto.OrderbookSnapshot;
import com.hft.shared.error.BusinessException;
import com.hft.shared.error.ErrorCode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/** Reads orderbook snapshots written by the Rust matching engine. */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderbookSnapshotReader {

    private final ReactiveStringRedisTemplate redis;
    private final ObjectMapper objectMapper;

    public Mono<OrderbookSnapshot> snapshot(String symbol) {
        return redis.opsForValue().get("orderbook:" + symbol)
                .switchIfEmpty(Mono.error(BusinessException.notFound(
                        ErrorCode.SYMBOL_NOT_FOUND, "No snapshot for " + symbol)))
                .flatMap(json -> {
                    try {
                        return Mono.just(objectMapper.readValue(json, OrderbookSnapshot.class));
                    } catch (Exception e) {
                        log.warn("Orderbook snapshot parse failed for {}: {}", symbol, e.getMessage());
                        return Mono.error(BusinessException.internal(
                                ErrorCode.INTERNAL_ERROR, "Snapshot parse failed"));
                    }
                });
    }
}
