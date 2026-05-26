package com.hft.kafka.dlq;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;

/** Admin endpoint exposing DLQ replay. Mount only in operator/internal services. */
@RestController
@RequestMapping("/admin/dlq")
@RequiredArgsConstructor
public class DLQAdminController {

    private final DLQReplayService replayService;

    /**
     * Replay messages from DLQ topic back to original topic.
     *
     * @param topic       DLQ topic name (must start with dlq.)
     * @param maxMessages cap on replay batch
     * @param timeoutSec  drain timeout
     */
    @PostMapping("/replay")
    public Mono<Map<String, Object>> replay(
            @RequestParam String topic,
            @RequestParam(defaultValue = "100") int maxMessages,
            @RequestParam(defaultValue = "30") int timeoutSec
    ) {
        return replayService.replay(topic, maxMessages, Duration.ofSeconds(timeoutSec))
                .map(n -> Map.of("topic", topic, "replayed", (Object) n));
    }
}
