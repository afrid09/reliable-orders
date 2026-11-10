package io.github.afrid09.reliableorders.order;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.afrid09.reliableorders.api.CreateOrderRequest;
import io.github.afrid09.reliableorders.outbox.OutboxRepository;
import io.github.afrid09.reliableorders.tracing.TraceContextCodec;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {

    private final OrderRepository orders;
    private final OutboxRepository outbox;
    private final TraceContextCodec traceContext;
    private final ObjectMapper objectMapper;

    public OrderService(OrderRepository orders, OutboxRepository outbox,
                        TraceContextCodec traceContext, ObjectMapper objectMapper) {
        this.orders = orders;
        this.outbox = outbox;
        this.traceContext = traceContext;
        this.objectMapper = objectMapper;
    }

    /**
     * Writes the order and its event in one transaction. We never call Kafka here:
     * if we did, a crash between the commit and the send would lose the event, and a
     * send followed by a failed commit would announce an order that does not exist.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public Order createOrder(CreateOrderRequest request) {
        Order order = new Order(
                UUID.randomUUID(),
                request.customerId(),
                request.sku(),
                request.quantity(),
                request.amountCents(),
                OrderStatus.PENDING,
                OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS));
        orders.insert(order);

        OrderCreatedEvent event = new OrderCreatedEvent(
                UUID.randomUUID(), order.id(), order.customerId(), order.sku(),
                order.quantity(), order.amountCents());
        outbox.append(event.eventId(), "Order", order.id().toString(), OrderCreatedEvent.TYPE,
                toJson(event), traceContext.currentTraceparent());

        return order;
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialise event", e);
        }
    }
}
