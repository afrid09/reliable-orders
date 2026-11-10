package io.github.afrid09.reliableorders.idempotency;

/** The client sent a different request body with an idempotency key it already used. */
public class IdempotencyKeyReusedException extends RuntimeException {

    public IdempotencyKeyReusedException(String key) {
        super("Idempotency key '" + key + "' was already used with a different request body");
    }
}
