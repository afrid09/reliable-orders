package io.github.afrid09.reliableorders.fulfillment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.afrid09.reliableorders.order.OrderCreatedEvent;
import io.github.afrid09.reliableorders.order.OrderRepository;
import io.github.afrid09.reliableorders.outbox.OutboxRelay;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Applies order events exactly once, on top of at-least-once delivery.
 *
 * <p>The dedup marker, the downstream call's result and the order status change are
 * committed in one database transaction. If anything fails, all of it rolls back,
 * including the dedup marker, so the retry starts clean.
 */
@Component
public class OrderEventsListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventsListener.class);

    private final ObjectMapper objectMapper;
    private final FulfillmentGateway gateway;
    private final OrderRepository orders;
    private final JdbcTemplate jdbc;
    private final Counter duplicates;
    private final Counter applied;

    public OrderEventsListener(ObjectMapper objectMapper, FulfillmentGateway gateway,
                               OrderRepository orders, JdbcTemplate jdbc, MeterRegistry meters) {
        this.objectMapper = objectMapper;
        this.gateway = gateway;
        this.orders = orders;
        this.jdbc = jdbc;
        this.duplicates = Counter.builder("fulfillment.events.duplicate")
                .description("Redelivered events that were skipped")
                .register(meters);
        this.applied = Counter.builder("fulfillment.events.applied")
                .description("Events applied for the first time")
                .register(meters);
    }

    @KafkaListener(topics = "${app.topics.order-events}")
    @Transactional
    public void onOrderEvent(ConsumerRecord<String, String> record) {
        String eventType = header(record, OutboxRelay.EVENT_TYPE_HEADER);
        if (eventType != null && !OrderCreatedEvent.TYPE.equals(eventType)) {
            log.debug("Ignoring event type {}", eventType);
            return;
        }

        OrderCreatedEvent event = parse(record.value());

        boolean firstDelivery = jdbc.update(
                "INSERT INTO processed_events (event_id) VALUES (?) ON CONFLICT DO NOTHING",
                event.eventId()) == 1;
        if (!firstDelivery) {
            duplicates.increment();
            log.info("Skipping duplicate delivery of event {}", event.eventId());
            return;
        }

        gateway.reserve(event);
        jdbc.update("INSERT INTO fulfillments (order_id) VALUES (?)", event.orderId());
        orders.markConfirmed(event.orderId());
        applied.increment();
    }

    private OrderCreatedEvent parse(String payload) {
        OrderCreatedEvent event;
        try {
            event = objectMapper.readValue(payload, OrderCreatedEvent.class);
        } catch (JsonProcessingException e) {
            throw new NonRetryableEventException("Event payload is not valid JSON", e);
        }
        if (event == null || event.eventId() == null || event.orderId() == null) {
            throw new NonRetryableEventException("Event is missing eventId or orderId");
        }
        return event;
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
