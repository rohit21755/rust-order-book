package com.hft.portfolio.repository;

import com.hft.portfolio.domain.Holding;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Repository
public interface HoldingRepository extends ReactiveCrudRepository<Holding, UUID> {
    Mono<Holding> findByUserIdAndSymbol(UUID userId, String symbol);
    Flux<Holding> findByUserId(UUID userId);
}
