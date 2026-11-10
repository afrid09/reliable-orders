package io.github.afrid09.reliableorders.outbox;

import io.github.afrid09.reliableorders.config.AppProperties;
import io.github.afrid09.reliableorders.tracing.TraceContextCodec;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Moves committed events from the outbox table to Kafka.
 *
 * <p>Delivery is at-least-once: if the process dies after Kafka acknowledges a record
 * but before {@code published_at} is committed, the event is sent again on the next
 * run. Consumers deduplicate on the {@code event-id} header.
 */
@Component
public class OutboxRelay {

    public static final String EVENT_ID_HEADER = "event-id";
    public static final String EVENT_TYPE_HEADER = "event-type";

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository outbox;
    private final KafkaTemplate<String, String> kafka;
    private final TransactionTemplate transactions;
    private final TraceContextCodec traceContext;
    private final Tracer tracer;
    private final AppProperties properties;

    public OutboxRelay(OutboxRepository outbox, KafkaTemplate<String, String> kafka,
                       TransactionTemplate transactions, TraceContextCodec traceContext,
                       Tracer tracer, AppProperties properties) {
        this.outbox = outbox;
        this.kafka = kafka;
        this.transactions = transactions;
        this.traceContext = traceContext;
        this.tracer = tracer;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${app.outbox.poll-interval-ms:500}")
    public void publishPending() {
        int batchSize = properties.outbox().batchSize();
        Integer published;
        do {
            published = transactions.execute(status -> publishBatch(batchSize));
        } while (published != null && published == batchSize);
    }

    private int publishBatch(int batchSize) {
        List<OutboxEvent> batch = outbox.lockUnpublishedBatch(batchSize);
        int published = 0;
        for (OutboxEvent event : batch) {
            try {
                send(event);
                outbox.markPublished(event.id());
                published++;
            } catch (RuntimeException e) {
                outbox.recordFailure(event.id(), e.getMessage());
                log.warn("Publishing outbox event {} failed (attempt {}), will retry: {}",
                        event.eventId(), event.attempts() + 1, e.getMessage());
                // Stop here so later events for the same order are not published out of order.
                break;
            }
        }
        return published;
    }

    private void send(OutboxEvent event) {
        Span span = traceContext.startSpan(event.traceparent(), "outbox publish");
        try (Tracer.SpanInScope ignored = tracer.withSpan(span)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    properties.topics().orderEvents(), event.aggregateId(), event.payload());
            record.headers().add(EVENT_ID_HEADER, event.eventId().toString().getBytes(StandardCharsets.UTF_8));
            record.headers().add(EVENT_TYPE_HEADER, event.eventType().getBytes(StandardCharsets.UTF_8));

            kafka.send(record).get(properties.outbox().sendTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            span.error(e);
            throw new IllegalStateException("Interrupted while publishing", e);
        } catch (ExecutionException | TimeoutException e) {
            span.error(e);
            throw new IllegalStateException("Kafka did not acknowledge event " + event.eventId(), e);
        } finally {
            span.end();
        }
    }
}
