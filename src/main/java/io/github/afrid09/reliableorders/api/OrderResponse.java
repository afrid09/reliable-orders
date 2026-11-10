package io.github.afrid09.reliableorders.api;

import io.github.afrid09.reliableorders.order.Order;
import java.time.OffsetDateTime;
import java.util.UUID;

public record OrderResponse(
        UUID id,
        String customerId,
        String sku,
        int quantity,
        long amountCents,
        String status,
        OffsetDateTime createdAt) {

    public static OrderResponse from(Order order) {
        return new OrderResponse(order.id(), order.customerId(), order.sku(), order.quantity(),
                order.amountCents(), order.status().name(), order.createdAt());
    }
}
