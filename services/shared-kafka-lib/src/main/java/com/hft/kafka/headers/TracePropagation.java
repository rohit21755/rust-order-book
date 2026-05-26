package com.hft.kafka.headers;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * W3C Trace Context inject/extract on Kafka record headers.
 *
 * <p>OpenTelemetry API classes are on the compile classpath via the {@code provided} scope —
 * if the service runtime does not include OTel, the SDK returns a no-op propagator and these
 * helpers become harmless no-ops.
 */
public final class TracePropagation {

    private static final TextMapSetter<Headers> SETTER = (h, key, value) -> {
        if (h == null || key == null || value == null) return;
        h.remove(key);
        h.add(key, value.getBytes(StandardCharsets.UTF_8));
    };

    private static final TextMapGetter<Headers> GETTER = new TextMapGetter<>() {
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

    private TracePropagation() {}

    /** Inject current trace context into outgoing Kafka headers. */
    public static void inject(Headers headers) {
        TextMapPropagator p = GlobalOpenTelemetry.getPropagators().getTextMapPropagator();
        p.inject(Context.current(), headers, SETTER);
    }

    /** Extract upstream trace context from incoming Kafka headers. */
    public static Context extract(Headers headers) {
        TextMapPropagator p = GlobalOpenTelemetry.getPropagators().getTextMapPropagator();
        return p.extract(Context.current(), headers, GETTER);
    }

    /**
     * Convenience: extract context, start a CONSUMER span, and return a closeable scope.
     * Caller pattern:
     * <pre>{@code
     * try (var s = TracePropagation.consumerSpan(record.headers(), "kafka.consume orders")) {
     *     ... process ...
     * }
     * }</pre>
     */
    public static AutoCloseable consumerSpan(Headers headers, String spanName) {
        Tracer tracer = GlobalOpenTelemetry.getTracer("com.hft.kafka");
        Context parent = extract(headers);
        Span span = tracer.spanBuilder(spanName)
                .setSpanKind(SpanKind.CONSUMER)
                .setParent(parent)
                .startSpan();
        Scope scope = span.makeCurrent();
        return () -> {
            scope.close();
            span.end();
        };
    }
}
