-- Orders: the business entity.
CREATE TABLE orders (
    id            UUID PRIMARY KEY,
    customer_id   TEXT        NOT NULL,
    sku           TEXT        NOT NULL,
    quantity      INT         NOT NULL CHECK (quantity > 0),
    amount_cents  BIGINT      NOT NULL CHECK (amount_cents >= 0),
    status        TEXT        NOT NULL DEFAULT 'PENDING',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Idempotency keys: one row per client-supplied key. The primary key is what
-- makes concurrent duplicates safe; the stored response is what makes retries
-- return exactly what the first request returned.
CREATE TABLE idempotency_keys (
    idempotency_key TEXT PRIMARY KEY,
    request_hash    TEXT        NOT NULL,
    response_status INT,
    response_body   TEXT,
    order_id        UUID REFERENCES orders (id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Transactional outbox: written in the same transaction as the order, so an
-- event exists if and only if the order was committed.
CREATE TABLE outbox_events (
    id             BIGSERIAL PRIMARY KEY,
    event_id       UUID        NOT NULL UNIQUE,
    aggregate_type TEXT        NOT NULL,
    aggregate_id   TEXT        NOT NULL,
    event_type     TEXT        NOT NULL,
    payload        TEXT        NOT NULL,
    traceparent    TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ,
    attempts       INT         NOT NULL DEFAULT 0,
    last_error     TEXT
);

-- The relay only ever scans unpublished rows, in insertion order.
CREATE INDEX outbox_events_unpublished_idx ON outbox_events (id) WHERE published_at IS NULL;

-- Consumer-side deduplication: delivery is at-least-once, so the consumer
-- records every event it has applied and skips repeats.
CREATE TABLE processed_events (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The side effect of fulfilment. UNIQUE(order_id) is a second line of defence.
CREATE TABLE fulfillments (
    id           BIGSERIAL PRIMARY KEY,
    order_id     UUID        NOT NULL UNIQUE REFERENCES orders (id),
    reserved_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
