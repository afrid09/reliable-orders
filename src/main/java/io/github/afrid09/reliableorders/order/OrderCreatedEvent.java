package io.github.afrid09.reliableorders.order;

import java.util.UUID;

/**
 * Published once per order. {@code eventId} is the deduplication key for consumers,
 * because delivery through Kafka is at-least-once.
 */
public record OrderCreatedEvent(
        UUID eventId,
        UUID orderId,
        String customerId,
        String sku,
        int quantity,
        long amountCents) {

    public static final String TYPE = "OrderCreated";
}
