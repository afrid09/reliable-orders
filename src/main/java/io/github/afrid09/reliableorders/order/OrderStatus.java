package io.github.afrid09.reliableorders.order;

public enum OrderStatus {
    /** Accepted and stored; fulfilment has not happened yet. */
    PENDING,
    /** Inventory reserved by the fulfilment consumer. */
    CONFIRMED
}
