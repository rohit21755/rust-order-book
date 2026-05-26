package com.hft.portfolio.repository;

import com.hft.portfolio.domain.ProcessedTrade;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProcessedTradeRepository extends ReactiveCrudRepository<ProcessedTrade, String> {
}
