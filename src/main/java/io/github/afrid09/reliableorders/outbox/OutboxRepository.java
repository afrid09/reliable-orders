package io.github.afrid09.reliableorders.outbox;

import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class OutboxRepository {

    private static final RowMapper<OutboxEvent> ROW_MAPPER = (rs, rowNum) -> new OutboxEvent(
            rs.getLong("id"),
            rs.getObject("event_id", UUID.class),
            rs.getString("aggregate_type"),
            rs.getString("aggregate_id"),
            rs.getString("event_type"),
            rs.getString("payload"),
            rs.getString("traceparent"),
            rs.getInt("attempts"));

    private final JdbcTemplate jdbc;

    public OutboxRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void append(UUID eventId, String aggregateType, String aggregateId,
                       String eventType, String payload, String traceparent) {
        jdbc.update("""
                INSERT INTO outbox_events (event_id, aggregate_type, aggregate_id, event_type, payload, traceparent)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                eventId, aggregateType, aggregateId, eventType, payload, traceparent);
    }

    /**
     * Locks the oldest unpublished events. {@code SKIP LOCKED} lets several relay
     * instances run at once: each takes a different batch instead of blocking or
     * publishing the same rows.
     */
    public List<OutboxEvent> lockUnpublishedBatch(int limit) {
        return jdbc.query("""
                SELECT * FROM outbox_events
                WHERE published_at IS NULL
                ORDER BY id
                LIMIT ?
                FOR UPDATE SKIP LOCKED
                """, ROW_MAPPER, limit);
    }

    public void markPublished(long id) {
        jdbc.update("UPDATE outbox_events SET published_at = now(), attempts = attempts + 1, last_error = NULL WHERE id = ?", id);
    }

    public void recordFailure(long id, String error) {
        jdbc.update("UPDATE outbox_events SET attempts = attempts + 1, last_error = ? WHERE id = ?",
                truncate(error, 2000), id);
    }

    private static String truncate(String value, int max) {
        return value == null || value.length() <= max ? value : value.substring(0, max);
    }
}
