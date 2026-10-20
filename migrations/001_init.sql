-- Receivers that events are delivered to.
CREATE TABLE endpoints (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    url         text        NOT NULL,
    secret      text        NOT NULL,
    is_demo     boolean     NOT NULL DEFAULT false,
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- At most one demo endpoint (the built-in flaky receiver).
CREATE UNIQUE INDEX endpoints_one_demo ON endpoints (is_demo) WHERE is_demo;

-- Events double as the job queue: each row is one webhook to deliver.
CREATE TABLE events (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    endpoint_id     uuid        NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
    type            text        NOT NULL,
    payload         jsonb       NOT NULL,
    status          text        NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'delivering', 'delivered', 'dead')),
    attempts        integer     NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    locked_until    timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    delivered_at    timestamptz
);

-- The worker's claim query filters on status and orders by next_attempt_at.
CREATE INDEX events_status_next_attempt ON events (status, next_attempt_at);

-- One row per HTTP attempt, successful or not.
CREATE TABLE delivery_attempts (
    id          bigserial PRIMARY KEY,
    event_id    uuid        NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    attempt_no  integer     NOT NULL,
    status_code integer,
    latency_ms  integer     NOT NULL,
    error       text,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX delivery_attempts_event ON delivery_attempts (event_id);
