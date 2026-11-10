package io.github.afrid09.reliableorders.fulfillment;

import io.github.afrid09.reliableorders.order.OrderCreatedEvent;

/** The downstream call fulfilment depends on, such as an inventory service. */
public interface FulfillmentGateway {

    /**
     * Reserves stock for the order.
     *
     * @throws TransientFulfillmentException when the dependency is temporarily unavailable
     */
    void reserve(OrderCreatedEvent event);
}
