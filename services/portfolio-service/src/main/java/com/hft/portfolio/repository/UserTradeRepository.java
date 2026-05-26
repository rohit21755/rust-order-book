package com.hft.portfolio.repository;

import com.hft.portfolio.domain.UserTrade;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

import java.util.UUID;

@Repository
public interface UserTradeRepository extends ReactiveCrudRepository<UserTrade, UUID> {

    @Query("SELECT * FROM portfolio_trades WHERE user_id = :userId ORDER BY executed_at DESC LIMIT :limit OFFSET :offset")
    Flux<UserTrade> findByUser(UUID userId, int limit, int offset);
}
