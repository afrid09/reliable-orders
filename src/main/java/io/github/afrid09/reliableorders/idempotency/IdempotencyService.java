package io.github.afrid09.reliableorders.idempotency;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.afrid09.reliableorders.api.CreateOrderRequest;
import io.github.afrid09.reliableorders.api.OrderResponse;
import io.github.afrid09.reliableorders.order.Order;
import io.github.afrid09.reliableorders.order.OrderService;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Makes order creation safe to retry.
 *
 * <p>The key claim, the order, the outbox event and the stored response are all
 * written in one transaction. Either all of them commit, or none do and the key is
 * free for the client's next attempt. A committed key always has a stored response,
 * so a retry gets back exactly what the first request got.
 */
@Service
public class IdempotencyService {

    public record Result(int status, OrderResponse body, boolean replayed) {
    }

    private final IdempotencyKeyRepository keys;
    private final OrderService orders;
    private final ObjectMapper objectMapper;

    public IdempotencyService(IdempotencyKeyRepository keys, OrderService orders, ObjectMapper objectMapper) {
        this.keys = keys;
        this.orders = orders;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public Result createOrder(String idempotencyKey, CreateOrderRequest request) {
        String requestHash = sha256(toJson(request));

        if (!keys.tryClaim(idempotencyKey, requestHash)) {
            return replay(idempotencyKey, requestHash);
        }

        Order order = orders.createOrder(request);
        OrderResponse response = OrderResponse.from(order);
        keys.complete(idempotencyKey, HttpStatus.CREATED.value(), toJson(response), order.id());
        return new Result(HttpStatus.CREATED.value(), response, false);
    }

    private Result replay(String idempotencyKey, String requestHash) {
        IdempotencyKeyRepository.StoredRequest stored = keys.find(idempotencyKey)
                .orElseThrow(() -> new IllegalStateException("Idempotency key vanished: " + idempotencyKey));

        if (!stored.requestHash().equals(requestHash)) {
            throw new IdempotencyKeyReusedException(idempotencyKey);
        }
        if (stored.responseStatus() == null) {
            // Cannot normally happen: keys only become visible together with their response.
            throw new IllegalStateException("Idempotency key has no stored response: " + idempotencyKey);
        }
        return new Result(stored.responseStatus(), fromJson(stored.responseBody()), true);
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialise value", e);
        }
    }

    private OrderResponse fromJson(String json) {
        try {
            return objectMapper.readValue(json, OrderResponse.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Stored response is not readable", e);
        }
    }

    private static String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
