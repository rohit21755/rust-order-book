package com.hft.kafka.headers;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/** Builds + reads standard Kafka headers. */
public final class MessageHeaders {

    private MessageHeaders() {}

    /** Create headers seeded with traceId/spanId/timestamp/service-origin. */
    public static Headers seed(String serviceOrigin) {
        return seed(serviceOrigin, UUID.randomUUID().toString(), UUID.randomUUID().toString().substring(0, 16));
    }

    public static Headers seed(String serviceOrigin, String traceId, String spanId) {
        Headers h = new RecordHeaders();
        put(h, HeaderKeys.TRACE_ID, traceId);
        put(h, HeaderKeys.SPAN_ID, spanId);
        put(h, HeaderKeys.TIMESTAMP, Instant.now().toString());
        put(h, HeaderKeys.SERVICE_ORIGIN, serviceOrigin);
        return h;
    }

    public static Headers withType(Headers h, String eventType) {
        put(h, HeaderKeys.EVENT_TYPE, eventType);
        return h;
    }

    public static Headers putAll(Headers h, Map<String, String> extras) {
        if (extras == null) return h;
        extras.forEach((k, v) -> put(h, k, v));
        return h;
    }

    public static void put(Headers h, String key, String value) {
        if (value == null) return;
        h.remove(key);
        h.add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));
    }

    public static String get(Headers h, String key) {
        Header header = h.lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    public static int getInt(Headers h, String key, int defaultValue) {
        String v = get(h, key);
        if (v == null) return defaultValue;
        try { return Integer.parseInt(v); } catch (NumberFormatException e) { return defaultValue; }
    }

    public static Map<String, String> toMap(Headers h) {
        Map<String, String> map = new HashMap<>();
        h.forEach(header -> map.put(header.key(), new String(header.value(), StandardCharsets.UTF_8)));
        return map;
    }
}
