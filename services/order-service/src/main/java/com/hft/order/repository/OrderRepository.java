package com.hft.order.repository;

import com.hft.order.domain.Order;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Repository
public interface OrderRepository extends ReactiveCrudRepository<Order, UUID> {

    Mono<Order> findByUserIdAndIdempotencyKey(UUID userId, String idempotencyKey);

    @Query("""
            SELECT COUNT(*) FROM orders
            WHERE user_id = :userId AND symbol = :symbol
              AND status IN ('PENDING_VALIDATION','VALIDATED','QUEUED','PARTIAL_FILL')
            """)
    Mono<Long> countOpenForUserSymbol(UUID userId, String symbol);

    @Query("""
            SELECT * FROM orders
            WHERE (:userId IS NULL OR user_id = :userId)
              AND (:symbol IS NULL OR symbol = :symbol)
              AND (:status IS NULL OR status = :status)
            ORDER BY created_at DESC
            LIMIT :limit OFFSET :offset
            """)
    Flux<Order> search(UUID userId, String symbol, String status, int limit, int offset);

    @Query("""
            SELECT COUNT(*) FROM orders
            WHERE (:userId IS NULL OR user_id = :userId)
              AND (:symbol IS NULL OR symbol = :symbol)
              AND (:status IS NULL OR status = :status)
            """)
    Mono<Long> count(UUID userId, String symbol, String status);

    @Query("""
            UPDATE orders SET status = :status, reject_reason = :reason, updated_at = NOW()
            WHERE id = :id
            """)
    Mono<Integer> updateStatus(UUID id, String status, String reason);
}
