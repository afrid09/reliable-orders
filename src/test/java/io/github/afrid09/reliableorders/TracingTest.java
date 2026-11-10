package io.github.afrid09.reliableorders;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.afrid09.reliableorders.api.OrderResponse;
import io.github.afrid09.reliableorders.config.AppProperties;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;

class TracingTest extends AbstractIntegrationTest {

    @Autowired
    ConsumerFactory<String, String> consumerFactory;

    @Autowired
    AppProperties properties;

    /**
     * The HTTP request and the Kafka message are handled on different threads,
     * possibly seconds apart, with a database table in between. They must still
     * belong to the same trace.
     */
    @Test
    void traceContinuesFromTheHttpRequestThroughTheOutboxIntoKafka() {
        OrderResponse order = postOrder(UUID.randomUUID().toString(), newOrder("WIDGET-1")).getBody();

        String storedTraceparent = jdbc.queryForObject(
                "SELECT traceparent FROM outbox_events WHERE aggregate_id = ?", String.class, order.id().toString());
        assertThat(storedTraceparent).as("trace context saved with the event").isNotBlank();

        ConsumerRecord<String, String> published = KafkaTestSupport.awaitRecord(
                consumerFactory, properties.topics().orderEvents(), order.id().toString(), Duration.ofSeconds(30));
        Header header = published.headers().lastHeader("traceparent");
        assertThat(header).as("trace context propagated on the Kafka record").isNotNull();

        String publishedTraceparent = new String(header.value(), StandardCharsets.UTF_8);
        assertThat(traceId(publishedTraceparent)).isEqualTo(traceId(storedTraceparent));
    }

    /** W3C traceparent is version-traceId-spanId-flags. */
    private static String traceId(String traceparent) {
        return traceparent.split("-")[1];
    }
}
