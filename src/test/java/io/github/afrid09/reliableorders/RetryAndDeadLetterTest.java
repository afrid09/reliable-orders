package io.github.afrid09.reliableorders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.afrid09.reliableorders.TestFulfillmentConfig.FlakyFulfillmentGateway;
import io.github.afrid09.reliableorders.api.OrderResponse;
import io.github.afrid09.reliableorders.config.AppProperties;
import io.github.afrid09.reliableorders.outbox.OutboxRelay;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;

class RetryAndDeadLetterTest extends AbstractIntegrationTest {

    @Autowired
    FlakyFulfillmentGateway gateway;

    @Autowired
    KafkaTemplate<String, String> kafka;

    @Autowired
    ConsumerFactory<String, String> consumerFactory;

    @Autowired
    AppProperties properties;

    @Test
    void transientFailureIsRetriedUntilItSucceeds() {
        OrderResponse order = postOrder(UUID.randomUUID().toString(), newOrder("FLAKY-WIDGET")).getBody();

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(orderStatus(order.id())).isEqualTo("CONFIRMED"));

        assertThat(gateway.callsFor(order.id()))
                .as("two failed attempts, then one that succeeded")
                .isEqualTo(FlakyFulfillmentGateway.FAILURES_BEFORE_SUCCESS + 1);
        assertThat(count("SELECT count(*) FROM fulfillments WHERE order_id = ?", order.id())).isEqualTo(1);
    }

    @Test
    void poisonMessageGoesToTheDeadLetterTopic() throws Exception {
        String key = "poison-" + UUID.randomUUID();
        String payload = "{this is not json";
        ProducerRecord<String, String> poison = new ProducerRecord<>(properties.topics().orderEvents(), key, payload);
        poison.headers().add(OutboxRelay.EVENT_TYPE_HEADER, "OrderCreated".getBytes(StandardCharsets.UTF_8));
        kafka.send(poison).get();

        ConsumerRecord<String, String> deadLetter = KafkaTestSupport.awaitRecord(
                consumerFactory, properties.topics().orderEventsDeadLetter(), key, Duration.ofSeconds(30));

        assertThat(deadLetter.value()).isEqualTo(payload);
        assertThat(headerText(deadLetter, KafkaHeaders.DLT_EXCEPTION_FQCN)
                + headerText(deadLetter, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN))
                .contains("NonRetryableEventException");
    }

    private static String headerText(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? "" : new String(header.value(), StandardCharsets.UTF_8);
    }
}
