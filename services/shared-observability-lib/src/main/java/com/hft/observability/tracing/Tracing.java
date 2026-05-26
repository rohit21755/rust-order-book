package com.hft.observability.tracing;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * Thin wrapper around the OTel API. Used by every service for manual spans
 * (order validation, risk check, matching engine call, PnL calc, etc.).
 *
 * <p>Standard attribute keys live on this class so service code spells them once.
 */
public final class Tracing {

    public static final AttributeKey<String> USER_ID = AttributeKey.stringKey("hft.user.id");
    public static final AttributeKey<String> ORDER_ID = AttributeKey.stringKey("hft.order.id");
    public static final AttributeKey<String> SYMBOL = AttributeKey.stringKey("hft.symbol");
    public static final AttributeKey<String> STAGE = AttributeKey.stringKey("hft.stage");
    public static final AttributeKey<String> ENVIRONMENT = AttributeKey.stringKey("deployment.environment");

    private static final String INSTRUMENTATION_NAME = "com.hft";

    private final Tracer tracer;
    private final String environment;

    public Tracing(OpenTelemetry otel, String environment) {
        this.tracer = otel.getTracer(INSTRUMENTATION_NAME);
        this.environment = environment;
    }

    /** Start a new INTERNAL span with the given name; attributes added via builder. */
    public SpanBuilder span(String name) {
        return tracer.spanBuilder(name)
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute(ENVIRONMENT, environment);
    }

    /** Run a Mono-producing function inside a span. Span closed on terminate/cancel. */
    public <T> Mono<T> wrap(String name, Function<Span, Mono<T>> work) {
        return wrap(name, Attributes.empty(), work);
    }

    public <T> Mono<T> wrap(String name, Attributes seedAttrs, Function<Span, Mono<T>> work) {
        return Mono.defer(() -> {
            Span span = span(name).setAllAttributes(seedAttrs).startSpan();
            try (Scope ignored = span.makeCurrent()) {
                return work.apply(span)
                        .doOnError(e -> {
                            span.recordException(e);
                            span.setStatus(StatusCode.ERROR, e.getMessage() == null ? "" : e.getMessage());
                        })
                        .doFinally(sig -> span.end());
            }
        });
    }

    /** Capture the current context (for handing to async work that needs to inherit it). */
    public Context currentContext() {
        return Context.current();
    }
}
