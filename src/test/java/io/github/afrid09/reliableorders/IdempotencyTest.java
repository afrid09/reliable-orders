package io.github.afrid09.reliableorders;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.afrid09.reliableorders.api.CreateOrderRequest;
import io.github.afrid09.reliableorders.api.OrderResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

class IdempotencyTest extends AbstractIntegrationTest {

    @Test
    void retryWithSameKeyReturnsTheOriginalResponseAndCreatesOneOrder() {
        String key = UUID.randomUUID().toString();
        CreateOrderRequest request = newOrder("WIDGET-1");

        ResponseEntity<OrderResponse> first = postOrder(key, request);
        ResponseEntity<OrderResponse> retry = postOrder(key, request);

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(first.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("false");

        assertThat(retry.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(retry.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("true");
        assertThat(retry.getBody()).isEqualTo(first.getBody());

        assertThat(count("SELECT count(*) FROM orders WHERE customer_id = ?", request.customerId())).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM outbox_events WHERE aggregate_id = ?",
                first.getBody().id().toString())).isEqualTo(1);
    }

    @Test
    void reusingAKeyWithADifferentBodyIsRejected() {
        String key = UUID.randomUUID().toString();
        postOrder(key, newOrder("WIDGET-1"));

        ResponseEntity<String> reused = postOrder(key, newOrder("WIDGET-2"), String.class);

        assertThat(reused.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
    }

    @Test
    void concurrentDuplicatesCreateExactlyOneOrder() throws Exception {
        String key = UUID.randomUUID().toString();
        CreateOrderRequest request = newOrder("WIDGET-1");
        int clients = 8;

        ExecutorService pool = Executors.newFixedThreadPool(clients);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<ResponseEntity<OrderResponse>>> futures = new ArrayList<>();
        for (int i = 0; i < clients; i++) {
            futures.add(pool.submit(() -> {
                start.await();
                return postOrder(key, request);
            }));
        }
        start.countDown();

        List<ResponseEntity<OrderResponse>> responses = new ArrayList<>();
        for (Future<ResponseEntity<OrderResponse>> future : futures) {
            responses.add(future.get(30, TimeUnit.SECONDS));
        }
        pool.shutdown();

        assertThat(responses).allSatisfy(r -> assertThat(r.getStatusCode()).isEqualTo(HttpStatus.CREATED));
        Set<UUID> orderIds = responses.stream().map(r -> r.getBody().id()).collect(Collectors.toSet());
        assertThat(orderIds).hasSize(1);
        assertThat(responses.stream().filter(r -> "false".equals(r.getHeaders().getFirst("Idempotent-Replayed"))))
                .as("exactly one request did the work; the rest were replays")
                .hasSize(1);
        assertThat(count("SELECT count(*) FROM orders WHERE customer_id = ?", request.customerId())).isEqualTo(1);
    }

    @Test
    void requestWithoutAnIdempotencyKeyIsRejected() {
        ResponseEntity<String> response = postOrder(null, newOrder("WIDGET-1"), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
}
