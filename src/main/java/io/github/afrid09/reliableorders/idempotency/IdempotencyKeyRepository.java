package io.github.afrid09.reliableorders.idempotency;

import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class IdempotencyKeyRepository {

    public record StoredRequest(String requestHash, Integer responseStatus, String responseBody) {
    }

    private final JdbcTemplate jdbc;

    public IdempotencyKeyRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Claims the key for this transaction. Returns {@code false} if the key already
     * exists. If another transaction is still holding the same key, Postgres makes
     * this insert wait until that transaction commits or rolls back, which is what
     * serialises concurrent duplicates without any application-level locking.
     */
    public boolean tryClaim(String key, String requestHash) {
        return jdbc.update("""
                INSERT INTO idempotency_keys (idempotency_key, request_hash)
                VALUES (?, ?)
                ON CONFLICT (idempotency_key) DO NOTHING
                """, key, requestHash) == 1;
    }

    public Optional<StoredRequest> find(String key) {
        return jdbc.query("""
                SELECT request_hash, response_status, response_body
                FROM idempotency_keys WHERE idempotency_key = ?
                """,
                (rs, rowNum) -> new StoredRequest(
                        rs.getString("request_hash"),
                        (Integer) rs.getObject("response_status"),
                        rs.getString("response_body")),
                key).stream().findFirst();
    }

    public void complete(String key, int status, String body, UUID orderId) {
        jdbc.update("""
                UPDATE idempotency_keys
                SET response_status = ?, response_body = ?, order_id = ?
                WHERE idempotency_key = ?
                """, status, body, orderId, key);
    }
}
