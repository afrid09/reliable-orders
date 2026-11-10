package io.github.afrid09.reliableorders.order;

import java.time.OffsetDateTime;
import java.util.UUID;

public record Order(
        UUID id,
        String customerId,
        String sku,
        int quantity,
        long amountCents,
        OrderStatus status,
        OffsetDateTime createdAt) {
}
