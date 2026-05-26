package com.hft.order.cqrs.replay;

import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * Admin replay endpoint. ADMIN-gated.
 *
 * <pre>
 * POST /admin/replay?fromSequence=0&aggregateType=ORDER&batchSize=10000
 * </pre>
 */
@RestController
@RequestMapping("/admin")
@RequiredArgsConstructor
public class ReplayController {

    private final ReplayService replayService;

    @PostMapping("/replay")
    @PreAuthorize("hasRole('ADMIN')")
    public Mono<Map<String, Object>> replay(
            @RequestParam(defaultValue = "0") long fromSequence,
            @RequestParam(defaultValue = "ORDER") String aggregateType,
            @RequestParam(defaultValue = "10000") int batchSize) {
        return replayService.replay(aggregateType, fromSequence, batchSize)
                .map(processed -> Map.of(
                        "aggregateType", aggregateType,
                        "fromSequence", (Object) fromSequence,
                        "processed", processed));
    }
}
