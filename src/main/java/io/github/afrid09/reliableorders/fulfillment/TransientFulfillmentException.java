package io.github.afrid09.reliableorders.fulfillment;

/** A downstream dependency is temporarily unavailable. Retrying should help. */
public class TransientFulfillmentException extends RuntimeException {

    public TransientFulfillmentException(String message) {
        super(message);
    }
}
