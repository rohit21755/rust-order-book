package com.hft.portfolio.repository;

import com.hft.portfolio.domain.PnlRecord;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

import java.time.OffsetDateTime;
import java.util.UUID;

@Repository
public interface PnlRecordRepository extends ReactiveCrudRepository<PnlRecord, UUID> {

    Flux<PnlRecord> findByUserId(UUID userId);

    @Query("""
            SELECT * FROM portfolio_pnl_records
            WHERE user_id = :userId AND timestamp >= :from AND timestamp <= :to
            ORDER BY timestamp ASC
            """)
    Flux<PnlRecord> history(UUID userId, OffsetDateTime from, OffsetDateTime to);
}
