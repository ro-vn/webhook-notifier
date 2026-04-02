CREATE TABLE IF NOT EXISTS webhook_subscriptions (
    id          SERIAL PRIMARY KEY,
    account_id  VARCHAR(255) NOT NULL,
    url         TEXT         NOT NULL,
    event_type  VARCHAR(255) NOT NULL,
    active      BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ws_account_event
    ON webhook_subscriptions (account_id, event_type)
    WHERE active = TRUE;

CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id            BIGSERIAL    PRIMARY KEY,
    event_id      VARCHAR(36)  NOT NULL,
    account_id    VARCHAR(255) NOT NULL,
    webhook_url   TEXT         NOT NULL,
    status        VARCHAR(50)  NOT NULL,
    attempts      INT          NOT NULL DEFAULT 1,
    error_message TEXT,
    delivered_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dl_account ON dead_letter_queue (account_id);
CREATE INDEX IF NOT EXISTS idx_dl_event   ON dead_letter_queue (event_id);
