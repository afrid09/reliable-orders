package io.github.afrid09.reliableorders.fulfillment;

import io.github.afrid09.reliableorders.order.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Stand-in for a real inventory service call. */
@Component
public class InventoryFulfillmentGateway implements FulfillmentGateway {

    private static final Logger log = LoggerFactory.getLogger(InventoryFulfillmentGateway.class);

    @Override
    public void reserve(OrderCreatedEvent event) {
        log.info("Reserved {} x {} for order {}", event.quantity(), event.sku(), event.orderId());
    }
}
