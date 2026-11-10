package io.github.afrid09.reliableorders.tracing;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import java.util.HashMap;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * Carries trace context across the outbox table.
 *
 * <p>The request that creates an order and the relay that later publishes its event
 * run on different threads, often seconds apart. Without help, the trace stops at
 * the database. We store the W3C {@code traceparent} next to the event and restore
 * it when publishing, so one trace covers HTTP request, outbox, Kafka and consumer.
 */
@Component
public class TraceContextCodec {

    private static final String TRACEPARENT = "traceparent";

    private final Tracer tracer;
    private final Propagator propagator;

    public TraceContextCodec(Tracer tracer, Propagator propagator) {
        this.tracer = tracer;
        this.propagator = propagator;
    }

    /** The current W3C traceparent, or {@code null} when no trace is active. */
    public String currentTraceparent() {
        TraceContext context = tracer.currentTraceContext().context();
        if (context == null) {
            return null;
        }
        Map<String, String> carrier = new HashMap<>();
        propagator.inject(context, carrier, Map::put);
        return carrier.get(TRACEPARENT);
    }

    /** Starts a span that continues the stored trace, or a new trace if there is none. */
    public Span startSpan(String traceparent, String name) {
        if (traceparent == null || traceparent.isBlank()) {
            return tracer.nextSpan().name(name).start();
        }
        Map<String, String> carrier = Map.of(TRACEPARENT, traceparent);
        return propagator.extract(carrier, Map::get).name(name).start();
    }
}
