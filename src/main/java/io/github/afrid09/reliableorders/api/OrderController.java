package io.github.afrid09.reliableorders.api;

import io.github.afrid09.reliableorders.idempotency.IdempotencyKeyReusedException;
import io.github.afrid09.reliableorders.idempotency.IdempotencyService;
import io.github.afrid09.reliableorders.order.OrderRepository;
import jakarta.validation.Valid;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OrderController {

    static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";
    static final String REPLAYED_HEADER = "Idempotent-Replayed";

    private final IdempotencyService idempotency;
    private final OrderRepository orders;

    public OrderController(IdempotencyService idempotency, OrderRepository orders) {
        this.idempotency = idempotency;
        this.orders = orders;
    }

    @PostMapping("/orders")
    public ResponseEntity<?> create(@RequestHeader(IDEMPOTENCY_KEY_HEADER) String idempotencyKey,
                                    @Valid @RequestBody CreateOrderRequest request) {
        if (idempotencyKey.isBlank() || idempotencyKey.length() > 255) {
            ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST,
                    IDEMPOTENCY_KEY_HEADER + " must be between 1 and 255 characters");
            return ResponseEntity.badRequest().body(problem);
        }

        IdempotencyService.Result result = idempotency.createOrder(idempotencyKey, request);
        return ResponseEntity.status(result.status())
                .header(REPLAYED_HEADER, String.valueOf(result.replayed()))
                .body(result.body());
    }

    @GetMapping("/orders/{id}")
    public ResponseEntity<OrderResponse> get(@PathVariable UUID id) {
        return ResponseEntity.of(orders.findById(id).map(OrderResponse::from));
    }

    @ExceptionHandler(IdempotencyKeyReusedException.class)
    ProblemDetail keyReused(IdempotencyKeyReusedException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.UNPROCESSABLE_ENTITY, e.getMessage());
    }
}
