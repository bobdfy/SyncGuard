CREATE TABLE outbox (
    id         BIGSERIAL PRIMARY KEY,
    payload    JSONB NOT NULL,           -- 要发的消息（JobMessage 的 JSON）
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at    TIMESTAMPTZ               -- NULL = 未发送
);