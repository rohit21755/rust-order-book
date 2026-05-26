package com.hft.order.readmodel;

import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

@Repository
public interface OrderReadModelRepository extends ReactiveCrudRepository<OrderReadModel, UUID> {

    @Query("""
            SELECT * FROM order_read_model
            WHERE (:userId IS NULL OR user_id = :userId)
              AND (:symbol IS NULL OR symbol = :symbol)
              AND (:status IS NULL OR status = :status)
            ORDER BY created_at DESC
            LIMIT :limit OFFSET :offset
            """)
    Flux<OrderReadModel> search(UUID userId, String symbol, String status, int limit, int offset);

    @Query("""
            SELECT COUNT(*) FROM order_read_model
            WHERE (:userId IS NULL OR user_id = :userId)
              AND (:symbol IS NULL OR symbol = :symbol)
              AND (:status IS NULL OR status = :status)
            """)
    Mono<Long> count(UUID userId, String symbol, String status);

    /**
     * Idempotent upsert keyed by order_id. The {@code WHERE last_sequence < :seq} clause
     * makes out-of-order projection deliveries safe — older events never overwrite newer.
     */
    @Query("""
            INSERT INTO order_read_model(
                order_id, user_id, symbol, side, type, price, stop_price, quantity,
                filled_quantity, avg_fill_price, status, idempotency_key, reject_reason,
                last_sequence, created_at, updated_at)
            VALUES (
                :orderId, :userId, :symbol, :side, :type, :price, :stopPrice, :quantity,
                :filledQty, :avgFillPrice, :status, :idempotencyKey, :rejectReason,
                :seq, :createdAt, :updatedAt)
            ON CONFLICT (order_id) DO UPDATE
                SET status = EXCLUDED.status,
                    filled_quantity = EXCLUDED.filled_quantity,
                    avg_fill_price = EXCLUDED.avg_fill_price,
                    reject_reason = EXCLUDED.reject_reason,
                    last_sequence = EXCLUDED.last_sequence,
                    updated_at = EXCLUDED.updated_at
            WHERE order_read_model.last_sequence < EXCLUDED.last_sequence
            """)
    Mono<Integer> upsert(UUID orderId, UUID userId, String symbol, String side, String type,
                         BigDecimal price, BigDecimal stopPrice, BigDecimal quantity,
                         BigDecimal filledQty, BigDecimal avgFillPrice, String status,
                         String idempotencyKey, String rejectReason, long seq,
                         OffsetDateTime createdAt, OffsetDateTime updatedAt);
}
