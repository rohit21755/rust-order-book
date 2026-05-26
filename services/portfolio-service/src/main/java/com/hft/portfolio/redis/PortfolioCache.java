package com.hft.portfolio.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.hft.portfolio.config.PortfolioProperties;
import com.hft.portfolio.dto.PortfolioDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.UUID;

/** Caches portfolio summary per user. Key: portfolio:{userId}, TTL 10s; invalidated on trade. */
@Slf4j
@Service
public class PortfolioCache {

    private final ReactiveStringRedisTemplate redis;
    private final PortfolioProperties props;
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

    public PortfolioCache(ReactiveStringRedisTemplate redis, PortfolioProperties props) {
        this.redis = redis;
        this.props = props;
    }

    private String key(UUID userId) {
        return "portfolio:" + userId;
    }

    public Mono<PortfolioDto> get(UUID userId) {
        return redis.opsForValue().get(key(userId))
                .flatMap(json -> {
                    try {
                        return Mono.just(mapper.readValue(json, PortfolioDto.class));
                    } catch (Exception e) {
                        return Mono.empty();
                    }
                });
    }

    public Mono<PortfolioDto> put(PortfolioDto dto) {
        Duration ttl = Duration.ofSeconds(props.getCache().getPortfolioTtlSeconds());
        try {
            return redis.opsForValue().set(key(dto.userId()), mapper.writeValueAsString(dto), ttl)
                    .thenReturn(dto);
        } catch (Exception e) {
            return Mono.just(dto);
        }
    }

    public Mono<Long> invalidate(UUID userId) {
        return redis.delete(key(userId));
    }
}
