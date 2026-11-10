package io.github.afrid09.reliableorders;

import io.github.afrid09.reliableorders.api.CreateOrderRequest;
import io.github.afrid09.reliableorders.api.OrderResponse;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Runs the whole application against real PostgreSQL and Kafka in containers.
 *
 * <p>The containers are started once and shared by every test class, so the Spring
 * context is created once too.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "app.outbox.poll-interval-ms=100",
                "app.consumer.retry.initial-interval=50ms",
                "management.otlp.tracing.export.enabled=false"
        })
@Import(TestFulfillmentConfig.class)
public abstract class AbstractIntegrationTest {

    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    @ServiceConnection
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

    static {
        POSTGRES.start();
        KAFKA.start();
    }

    @Autowired
    protected TestRestTemplate rest;

    @Autowired
    protected JdbcTemplate jdbc;

    protected static CreateOrderRequest newOrder(String sku) {
        return new CreateOrderRequest("customer-" + UUID.randomUUID(), sku, 2, 4_999);
    }

    protected ResponseEntity<OrderResponse> postOrder(String idempotencyKey, CreateOrderRequest request) {
        return postOrder(idempotencyKey, request, OrderResponse.class);
    }

    protected <T> ResponseEntity<T> postOrder(String idempotencyKey, CreateOrderRequest request, Class<T> type) {
        HttpHeaders headers = new HttpHeaders();
        if (idempotencyKey != null) {
            headers.set("Idempotency-Key", idempotencyKey);
        }
        return rest.exchange("/orders", HttpMethod.POST, new HttpEntity<>(request, headers), type);
    }

    protected String orderStatus(UUID orderId) {
        return jdbc.queryForObject("SELECT status FROM orders WHERE id = ?", String.class, orderId);
    }

    protected int count(String sql, Object... args) {
        Integer value = jdbc.queryForObject(sql, Integer.class, args);
        return value == null ? 0 : value;
    }
}
