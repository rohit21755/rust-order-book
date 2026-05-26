package com.hft.observability.tracing;

import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * W3C Trace Context propagator getters/setters over Kafka {@link Headers}.
 *
 * <p>Standard header names (`traceparent`, `tracestate`) are written by the OTel
 * {@code W3CTraceContextPropagator}; nothing custom is added.
 */
public final class KafkaTraceHeaders {

    public static final TextMapSetter<Headers> SETTER = (headers, key, value) -> {
        if (headers == null || key == null || value == null) return;
        headers.remove(key);
        headers.add(key, value.getBytes(StandardCharsets.UTF_8));
    };

    public static final TextMapGetter<Headers> GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Headers carrier) {
            List<String> keys = new ArrayList<>();
            for (Header h : carrier) keys.add(h.key());
            return keys;
        }

        @Override
        public String get(Headers carrier, String key) {
            if (carrier == null) return null;
            Header h = carrier.lastHeader(key);
            return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
        }
    };

    private KafkaTraceHeaders() {}
}
