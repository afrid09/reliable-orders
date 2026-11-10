package io.github.afrid09.reliableorders;

import io.github.afrid09.reliableorders.fulfillment.FulfillmentGateway;
import io.github.afrid09.reliableorders.fulfillment.TransientFulfillmentException;
import io.github.afrid09.reliableorders.order.OrderCreatedEvent;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@TestConfiguration(proxyBeanMethods = false)
public class TestFulfillmentConfig {

    @Bean
    @Primary
    FlakyFulfillmentGateway flakyFulfillmentGateway() {
        return new FlakyFulfillmentGateway();
    }

    /**
     * Fails the first two calls for any SKU starting with {@code FLAKY-}, the way a
     * downstream service does while it is restarting. Every other SKU succeeds.
     */
    public static class FlakyFulfillmentGateway implements FulfillmentGateway {

        public static final int FAILURES_BEFORE_SUCCESS = 2;

        private final Map<UUID, AtomicInteger> calls = new ConcurrentHashMap<>();

        @Override
        public void reserve(OrderCreatedEvent event) {
            int attempt = calls.computeIfAbsent(event.orderId(), id -> new AtomicInteger()).incrementAndGet();
            if (event.sku().startsWith("FLAKY-") && attempt <= FAILURES_BEFORE_SUCCESS) {
                throw new TransientFulfillmentException("Inventory service unavailable (attempt " + attempt + ")");
            }
        }

        public int callsFor(UUID orderId) {
            AtomicInteger count = calls.get(orderId);
            return count == null ? 0 : count.get();
        }
    }
}
