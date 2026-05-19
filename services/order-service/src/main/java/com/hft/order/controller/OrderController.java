package com.hft.order.controller;

import com.hft.order.dto.OrderRequest;
import com.hft.order.dto.OrderResponse;
import com.hft.order.dto.OrderbookSnapshot;
import com.hft.order.dto.PagedResponse;
import com.hft.order.redis.OrderbookSnapshotReader;
import com.hft.order.service.OrderService;
import com.hft.shared.security.AuthenticatedUserPrincipal;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;
    private final OrderbookSnapshotReader snapshotReader;

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

    @GetMapping("/book/{symbol}")
    public Mono<OrderbookSnapshot> book(@PathVariable String symbol) {
        return snapshotReader.snapshot(symbol);
    }

    private Mono<UUID> currentUserId() {
        return ReactiveSecurityContextHolder.getContext()
                .map(ctx -> (AuthenticatedUserPrincipal) ctx.getAuthentication())
                .map(AuthenticatedUserPrincipal::getUserId);
    }
}
