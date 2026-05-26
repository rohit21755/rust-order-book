package com.hft.kafka.event;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.Map;

/** Wrapper persisted to dlq.* topics when consumer retries are exhausted. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DLQMessage(
        String originalTopic,
        String originalKey,
        String originalPayload,
        String errorMessage,
        String errorClass,
        int retryCount,
        Instant timestamp,
        Map<String, String> headers
) implements EventEnvelope {

    @Override
    public String partitionKey() {
        return originalKey != null ? originalKey : originalTopic;
    }
}
