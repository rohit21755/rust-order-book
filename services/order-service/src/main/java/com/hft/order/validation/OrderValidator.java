package com.hft.order.validation;

import com.hft.order.config.OrderProperties;
import com.hft.order.domain.OrderType;
import com.hft.order.dto.OrderRequest;
import com.hft.order.repository.OrderRepository;
import com.hft.shared.error.BusinessException;
import com.hft.shared.error.ErrorCode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.Set;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class OrderValidator {

    private final OrderProperties props;
    private final OrderRepository orderRepository;

    /** Synchronous structural validation. Throws BusinessException on first failure. */
    public void validateStructure(OrderRequest req) {
        Set<String> supported = Set.copyOf(props.getSupportedSymbols());
        if (!supported.contains(req.symbol())) {
            throw BusinessException.badRequest(ErrorCode.INVALID_SYMBOL,
                    "Unsupported symbol: " + req.symbol());
        }
        BigDecimal min = props.getLimits().getMinQuantity();
        BigDecimal max = props.getLimits().getMaxPositionQuantity();
        if (req.quantity() == null || req.quantity().compareTo(min) < 0) {
            throw BusinessException.badRequest(ErrorCode.INVALID_QUANTITY,
                    "Quantity below minimum " + min);
        }
        if (req.quantity().compareTo(max) > 0) {
            throw BusinessException.badRequest(ErrorCode.POSITION_LIMIT_EXCEEDED,
                    "Quantity exceeds max position " + max);
        }
        if (req.type() == OrderType.LIMIT) {
            if (req.price() == null || req.price().signum() <= 0) {
                throw BusinessException.badRequest(ErrorCode.INVALID_PRICE,
                        "LIMIT order requires price > 0");
            }
        }
        if (req.type() == OrderType.STOP_LOSS) {
            if (req.stopPrice() == null || req.stopPrice().signum() <= 0) {
                throw BusinessException.badRequest(ErrorCode.INVALID_PRICE,
                        "STOP_LOSS order requires stopPrice > 0");
            }
        }
    }

    /** Async: enforce per-user-per-symbol open-order ceiling. */
    public Mono<Void> validateOpenOrderLimit(UUID userId, String symbol) {
        int max = props.getLimits().getMaxOpenOrdersPerSymbol();
        return orderRepository.countOpenForUserSymbol(userId, symbol)
                .flatMap(n -> n >= max
                        ? Mono.error(BusinessException.badRequest(ErrorCode.OPEN_ORDER_LIMIT_EXCEEDED,
                            "Open orders for " + symbol + " hit limit " + max))
                        : Mono.empty());
    }
}
