package io.github.afrid09.reliableorders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.afrid09.reliableorders.api.OrderResponse;
import io.github.afrid09.reliableorders.config.AppProperties;
import io.github.afrid09.reliableorders.order.Order;
import io.github.afrid09.reliableorders.order.OrderService;
import io.github.afrid09.reliableorders.outbox.OutboxRelay;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;

class OutboxTest extends AbstractIntegrationTest {

    @Autowired
    OrderService orderService;

    @Autowired
    TransactionTemplate transactions;

    @Autowired
    KafkaTemplate<String, String> kafka;

    @Autowired
    AppProperties properties;

    @Autowired
    MeterRegistry meters;

    @Test
    void committedOrderIsPublishedAndFulfilled() {
        OrderResponse order = postOrder(UUID.randomUUID().toString(), newOrder("WIDGET-1")).getBody();

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(orderStatus(order.id())).isEqualTo("CONFIRMED"));

        assertThat(count("SELECT count(*) FROM outbox_events WHERE aggregate_id = ? AND published_at IS NOT NULL",
                order.id().toString())).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM fulfillments WHERE order_id = ?", order.id())).isEqualTo(1);
    }

    @Test
    void rolledBackOrderLeavesNoEventBehind() {
        AtomicReference<UUID> orderId = new AtomicReference<>();

        assertThatThrownBy(() -> transactions.executeWithoutResult(status -> {
            Order order = orderService.createOrder(newOrder("WIDGET-1"));
            orderId.set(order.id());
            throw new IllegalStateException("Simulated crash after the order and event were written");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(orderId.get()).isNotNull();
        assertThat(count("SELECT count(*) FROM orders WHERE id = ?", orderId.get())).isZero();
        assertThat(count("SELECT count(*) FROM outbox_events WHERE aggregate_id = ?",
                orderId.get().toString())).isZero();
    }

    /**
     * The relay can crash after Kafka acknowledged an event but before it recorded
     * that, and then it publishes the event again. The consumer must apply it once.
     */
    @Test
    void redeliveredEventIsAppliedOnlyOnce() throws Exception {
        OrderResponse order = postOrder(UUID.randomUUID().toString(), newOrder("WIDGET-1")).getBody();
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(orderStatus(order.id())).isEqualTo("CONFIRMED"));

        Map<String, Object> event = jdbc.queryForMap(
                "SELECT event_id, payload FROM outbox_events WHERE aggregate_id = ?", order.id().toString());
        double duplicatesBefore = duplicateCount();

        for (int i = 0; i < 2; i++) {
            ProducerRecord<String, String> redelivery = new ProducerRecord<>(
                    properties.topics().orderEvents(), order.id().toString(), (String) event.get("payload"));
            redelivery.headers().add(OutboxRelay.EVENT_ID_HEADER,
                    event.get("event_id").toString().getBytes(StandardCharsets.UTF_8));
            redelivery.headers().add(OutboxRelay.EVENT_TYPE_HEADER,
                    "OrderCreated".getBytes(StandardCharsets.UTF_8));
            kafka.send(redelivery).get();
        }

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(duplicateCount() - duplicatesBefore).isEqualTo(2));
        assertThat(count("SELECT count(*) FROM fulfillments WHERE order_id = ?", order.id())).isEqualTo(1);
    }

    private double duplicateCount() {
        return meters.counter("fulfillment.events.duplicate").count();
    }
}
