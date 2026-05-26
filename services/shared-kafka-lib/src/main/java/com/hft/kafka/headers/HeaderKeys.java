package com.hft.kafka.headers;

/** Standard Kafka record header names used across the platform. */
public final class HeaderKeys {
    public static final String TRACE_ID = "x-trace-id";
    public static final String SPAN_ID = "x-span-id";
    public static final String TIMESTAMP = "x-timestamp";
    public static final String SERVICE_ORIGIN = "x-service-origin";
    public static final String EVENT_TYPE = "x-event-type";
    public static final String RETRY_COUNT = "x-retry-count";
    public static final String ORIGINAL_TOPIC = "x-original-topic";

    private HeaderKeys() {}
}
