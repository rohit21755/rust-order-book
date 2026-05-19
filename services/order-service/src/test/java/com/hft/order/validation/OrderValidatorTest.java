package com.hft.order.validation;

import com.hft.order.config.OrderProperties;
import com.hft.order.domain.OrderSide;
import com.hft.order.domain.OrderType;
import com.hft.order.dto.OrderRequest;
import com.hft.order.repository.OrderRepository;
import com.hft.shared.error.BusinessException;
import com.hft.shared.error.ErrorCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OrderValidatorTest {

    private OrderRepository repo;
    private OrderValidator validator;

    @BeforeEach
    void setUp() {
        repo = mock(OrderRepository.class);
        OrderProperties props = new OrderProperties();
        props.setSupportedSymbols(List.of("BTC-USDT", "ETH-USDT"));
        props.getLimits().setMaxOpenOrdersPerSymbol(2);
        props.getLimits().setMaxPositionQuantity(new BigDecimal("100"));
        props.getLimits().setMinQuantity(new BigDecimal("0.0001"));
        validator = new OrderValidator(props, repo);
    }

    @Test
    void rejectsUnknownSymbol() {
        OrderRequest req = new OrderRequest("WTF-USDT", OrderSide.BUY, OrderType.LIMIT,
                new BigDecimal("10"), null, new BigDecimal("1"), "k");
        assertThatThrownBy(() -> validator.validateStructure(req))
                .isInstanceOfSatisfying(BusinessException.class,
                        ex -> assert ex.getCode() == ErrorCode.INVALID_SYMBOL);
    }

    @Test
    void rejectsLimitWithoutPrice() {
        OrderRequest req = new OrderRequest("BTC-USDT", OrderSide.BUY, OrderType.LIMIT,
                null, null, new BigDecimal("1"), "k");
        assertThatThrownBy(() -> validator.validateStructure(req))
                .isInstanceOfSatisfying(BusinessException.class,
                        ex -> assert ex.getCode() == ErrorCode.INVALID_PRICE);
    }

    @Test
    void rejectsStopLossWithoutStopPrice() {
        OrderRequest req = new OrderRequest("BTC-USDT", OrderSide.SELL, OrderType.STOP_LOSS,
                null, null, new BigDecimal("1"), "k");
        assertThatThrownBy(() -> validator.validateStructure(req))
                .isInstanceOfSatisfying(BusinessException.class,
                        ex -> assert ex.getCode() == ErrorCode.INVALID_PRICE);
    }

    @Test
    void rejectsBelowMinQty() {
        OrderRequest req = new OrderRequest("BTC-USDT", OrderSide.BUY, OrderType.LIMIT,
                new BigDecimal("10"), null, new BigDecimal("0.00000001"), "k");
        assertThatThrownBy(() -> validator.validateStructure(req))
                .isInstanceOfSatisfying(BusinessException.class,
                        ex -> assert ex.getCode() == ErrorCode.INVALID_QUANTITY);
    }

    @Test
    void rejectsAboveMaxPosition() {
        OrderRequest req = new OrderRequest("BTC-USDT", OrderSide.BUY, OrderType.LIMIT,
                new BigDecimal("10"), null, new BigDecimal("1000"), "k");
        assertThatThrownBy(() -> validator.validateStructure(req))
                .isInstanceOfSatisfying(BusinessException.class,
                        ex -> assert ex.getCode() == ErrorCode.POSITION_LIMIT_EXCEEDED);
    }

    @Test
    void acceptsValidLimit() {
        OrderRequest req = new OrderRequest("BTC-USDT", OrderSide.BUY, OrderType.LIMIT,
                new BigDecimal("10"), null, new BigDecimal("1"), "k");
        validator.validateStructure(req);
    }

    @Test
    void openOrderLimitTriggers() {
        UUID uid = UUID.randomUUID();
        when(repo.countOpenForUserSymbol(any(UUID.class), anyString())).thenReturn(Mono.just(2L));

        StepVerifier.create(validator.validateOpenOrderLimit(uid, "BTC-USDT"))
                .expectErrorMatches(t -> t instanceof BusinessException be
                        && be.getCode() == ErrorCode.OPEN_ORDER_LIMIT_EXCEEDED)
                .verify();
    }

    @Test
    void openOrderLimitPasses() {
        UUID uid = UUID.randomUUID();
        when(repo.countOpenForUserSymbol(any(UUID.class), anyString())).thenReturn(Mono.just(1L));

        StepVerifier.create(validator.validateOpenOrderLimit(uid, "BTC-USDT")).verifyComplete();
    }
}
