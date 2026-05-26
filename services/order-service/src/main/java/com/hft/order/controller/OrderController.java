package com.hft.order.controller;

import com.hft.grpc.client.MatchingEngineGrpcClient;
import com.hft.matching.grpc.OrderBookResponse;
import com.hft.order.dto.OrderRequest;
import com.hft.order.dto.OrderResponse;
import com.hft.order.dto.OrderbookSnapshot;
import com.hft.order.dto.PagedResponse;
import com.hft.order.service.OrderService;
import com.hft.shared.security.AuthenticatedUserPrincipal;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;
    private final MatchingEngineGrpcClient engineClient;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<OrderResponse> submit(@Valid @RequestBody OrderRequest req) {
        return currentUserId().flatMap(uid -> orderService.submit(uid, req));
    }

    @GetMapping("/{orderId}")
    public Mono<OrderResponse> get(@PathVariable UUID orderId) {
        return orderService.get(orderId);
    }

    @GetMapping
    public Mono<PagedResponse<OrderResponse>> list(
            @RequestParam(required = false) UUID userId,
            @RequestParam(required = false) String symbol,
            @RequestParam(required = false) String status,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size
    ) {
        int boundedSize = Math.max(1, Math.min(size, 200));
        return orderService.search(userId, symbol, status, page, boundedSize).collectList()
                .zipWith(orderService.count(userId, symbol, status))
                .map(t -> new PagedResponse<>(t.getT1(), t.getT2(), page, boundedSize));
    }

    @DeleteMapping("/{orderId}")
    public Mono<OrderResponse> cancel(@PathVariable UUID orderId) {
        return currentUserId().flatMap(uid -> orderService.cancel(uid, orderId));
    }

    /**
     * Orderbook snapshot. Primary path = gRPC to matching engine (low latency, live state).
     * On engine failure / open circuit, the gRPC client transparently falls back to the
     * Redis snapshot (wired via RedisOrderbookFallback bean).
     */
    @GetMapping("/book/{symbol}")
    public Mono<OrderbookSnapshot> book(
            @PathVariable String symbol,
            @RequestParam(defaultValue = "20") int depth
    ) {
        int boundedDepth = Math.max(1, Math.min(depth, 100));
        return engineClient.getOrderBook(symbol, boundedDepth).map(OrderController::toDto);
    }

    private static OrderbookSnapshot toDto(OrderBookResponse r) {
        List<OrderbookSnapshot.Level> bids = r.getBidsList().stream()
                .map(l -> new OrderbookSnapshot.Level(new BigDecimal(l.getPrice()), new BigDecimal(l.getQuantity())))
                .toList();
        List<OrderbookSnapshot.Level> asks = r.getAsksList().stream()
                .map(l -> new OrderbookSnapshot.Level(new BigDecimal(l.getPrice()), new BigDecimal(l.getQuantity())))
                .toList();
        return new OrderbookSnapshot(r.getSymbol(), r.getSequence(), bids, asks);
    }

    private Mono<UUID> currentUserId() {
        return ReactiveSecurityContextHolder.getContext()
                .map(ctx -> (AuthenticatedUserPrincipal) ctx.getAuthentication())
                .map(AuthenticatedUserPrincipal::getUserId);
    }
}
