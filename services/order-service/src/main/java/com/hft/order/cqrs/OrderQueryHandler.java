package com.hft.order.cqrs;

import com.hft.order.dto.OrderResponse;
import com.hft.order.readmodel.OrderReadModel;
import com.hft.order.readmodel.OrderReadModelRepository;
import com.hft.shared.error.BusinessException;
import com.hft.shared.error.ErrorCode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

/** Read-side: queries the denormalized order_read_model table. */
@Service
@RequiredArgsConstructor
public class OrderQueryHandler {

    private final OrderReadModelRepository repo;

    public Mono<OrderResponse> get(UUID orderId) {
        return repo.findById(orderId)
                .switchIfEmpty(Mono.error(BusinessException.notFound(ErrorCode.ORDER_NOT_FOUND, "Order not found")))
                .map(OrderQueryHandler::toResponse);
    }

    public Flux<OrderResponse> search(UUID userId, String symbol, String status, int page, int size) {
        int bounded = Math.max(1, Math.min(size, 200));
        return repo.search(userId, symbol, status, bounded, page * bounded).map(OrderQueryHandler::toResponse);
    }

    public Mono<Long> count(UUID userId, String symbol, String status) {
        return repo.count(userId, symbol, status);
    }

    private static OrderResponse toResponse(OrderReadModel r) {
        return new OrderResponse(
                r.getOrderId(), r.getUserId(), r.getSymbol(), r.getSide(), r.getType(),
                r.getPrice(), r.getStopPrice(), r.getQuantity(),
                r.getFilledQuantity(), r.getAvgFillPrice(), r.getStatus(),
                r.getIdempotencyKey(), r.getRejectReason(),
                r.getCreatedAt(), r.getUpdatedAt());
    }
}
