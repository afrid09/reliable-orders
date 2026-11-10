package io.github.afrid09.reliableorders.api;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Size;

public record CreateOrderRequest(
        @NotBlank @Size(max = 64) String customerId,
        @NotBlank @Size(max = 64) String sku,
        @Min(1) int quantity,
        @PositiveOrZero long amountCents) {
}
