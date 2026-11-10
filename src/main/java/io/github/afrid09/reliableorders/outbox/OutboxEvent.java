package io.github.afrid09.reliableorders.outbox;

import java.util.UUID;

public record OutboxEvent(
        long id,
        UUID eventId,
        String aggregateType,
        String aggregateId,
        String eventType,
        String payload,
        String traceparent,
        int attempts) {
}
