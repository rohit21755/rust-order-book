package com.hft.order.redis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.order.config.OrderProperties;
import com.hft.order.dto.OrderResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.UUID;

/** Cache order status snapshots. Key: order:{orderId}. */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderCache {

    private final ReactiveStringRedisTemplate redis;
    private final OrderProperties props;
    private final ObjectMapper objectMapper;

    public Mono<Boolean> put(OrderResponse o) {
        Duration ttl = Duration.ofHours(props.getCache().getOrderTtlHours());
        return Mono.fromCallable(() -> objectMapper.writeValueAsString(o))
                .flatMap(json -> redis.opsForValue().set("order:" + o.id(), json, ttl))
                .onErrorResume(JsonProcessingException.class, e -> {
                    log.warn("Order cache serialize failed: {}", e.getMessage());
                    return Mono.just(false);
                });
    }

    public Mono<OrderResponse> get(UUID orderId) {
        return redis.opsForValue().get("order:" + orderId)
                .flatMap(json -> {
                    try {
                        return Mono.just(objectMapper.readValue(json, OrderResponse.class));
                    } catch (Exception e) {
                        return Mono.empty();
                    }
                });
    }

    public Mono<Long> evict(UUID orderId) {
        return redis.delete("order:" + orderId);
    }
}
