package io.github.afrid09.reliableorders.fulfillment;

/**
 * The record can never be processed, however often we retry it (bad JSON, missing
 * fields). It goes straight to the dead-letter topic.
 */
public class NonRetryableEventException extends RuntimeException {

    public NonRetryableEventException(String message, Throwable cause) {
        super(message, cause);
    }

    public NonRetryableEventException(String message) {
        super(message);
    }
}
