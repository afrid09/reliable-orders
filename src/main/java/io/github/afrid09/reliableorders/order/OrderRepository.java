package io.github.afrid09.reliableorders.order;

import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class OrderRepository {

    private static final RowMapper<Order> ROW_MAPPER = (rs, rowNum) -> new Order(
            rs.getObject("id", UUID.class),
            rs.getString("customer_id"),
            rs.getString("sku"),
            rs.getInt("quantity"),
            rs.getLong("amount_cents"),
            OrderStatus.valueOf(rs.getString("status")),
            rs.getObject("created_at", OffsetDateTime.class));

    private final JdbcTemplate jdbc;

    public OrderRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(Order order) {
        jdbc.update("""
                INSERT INTO orders (id, customer_id, sku, quantity, amount_cents, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                order.id(), order.customerId(), order.sku(), order.quantity(),
                order.amountCents(), order.status().name(), order.createdAt());
    }

    public Optional<Order> findById(UUID id) {
        return jdbc.query("SELECT * FROM orders WHERE id = ?", ROW_MAPPER, id).stream().findFirst();
    }

    public void markConfirmed(UUID id) {
        jdbc.update("UPDATE orders SET status = ? WHERE id = ?", OrderStatus.CONFIRMED.name(), id);
    }
}
