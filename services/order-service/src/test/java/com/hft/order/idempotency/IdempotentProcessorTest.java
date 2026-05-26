package com.hft.order.idempotency;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class IdempotentProcessorTest {

    ProcessedEventRepository repo;
    IdempotentProcessor proc;

    @BeforeEach
    void setUp() {
        repo = mock(ProcessedEventRepository.class);
        proc = new IdempotentProcessor(repo);
    }

    @Test
    void runsWorkOnFirstClaim() {
        when(repo.tryClaim(anyString(), anyString())).thenReturn(Mono.just(1));
        AtomicInteger calls = new AtomicInteger();
        StepVerifier.create(proc.process("grp", "evt-1", () -> {
            calls.incrementAndGet();
            return Mono.empty();
        })).verifyComplete();
        assert calls.get() == 1;
    }

    @Test
    void skipsWorkOnDuplicate() {
        when(repo.tryClaim(anyString(), anyString())).thenReturn(Mono.just(0));
        AtomicInteger calls = new AtomicInteger();
        StepVerifier.create(proc.process("grp", "evt-1", () -> {
            calls.incrementAndGet();
            return Mono.empty();
        })).verifyComplete();
        assert calls.get() == 0;
    }
}
