-- 1. users — 用户
CREATE TABLE users (
    id            SERIAL PRIMARY KEY,
    username      VARCHAR NOT NULL UNIQUE,
    password_hash VARCHAR NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- 2. connections — 数据源连接（每个连接归属一个用户）
CREATE TABLE connections (
    id          SERIAL PRIMARY KEY,
    user_id     INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name        VARCHAR NOT NULL,
    source_type VARCHAR NOT NULL,   -- mock / github ...
    source_url  VARCHAR NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- 3. sync_jobs — 同步任务（断点 / 进度 / 状态都在这一张表）
CREATE TABLE sync_jobs (
    id                   SERIAL PRIMARY KEY,
    user_id              INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    connection_id        INT NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
    target_connection_id INT REFERENCES connections(id) ON DELETE SET NULL,  -- 可空：默认写入内部存储
    task_name            VARCHAR NOT NULL,
    sync_content         TEXT NOT NULL DEFAULT '',
    status               VARCHAR NOT NULL DEFAULT 'pending',
    cursor               TEXT NOT NULL DEFAULT '',
    total_count          INT NOT NULL DEFAULT 0,
    error_msg            TEXT NOT NULL DEFAULT '',
    started_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at          TIMESTAMPTZ
);


-- 4. synced_records — 同步记录落地（每用户的目标端镜像）
-- 复合主键 (connection_id, id)：同一数据源的同一条记录，在不同
-- connection（不同用户 / 同一用户多个源连接）下各自独立，互不覆盖。
-- user_id 是冗余列（等价于 connections.user_id），仅为查询免 JOIN。
CREATE TABLE synced_records (
    connection_id INT NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
    id            TEXT NOT NULL,              -- 源端记录 ID（在 connection 内唯一）
    user_id       INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    version       INTEGER NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,
    data          JSONB NOT NULL DEFAULT '{}',
    synced_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (connection_id, id)
);


-- 索引：覆盖代码里的实际查询路径
CREATE INDEX idx_synced_records_user_id ON synced_records (user_id);    -- ListRecordsByUser
CREATE INDEX idx_sync_jobs_user_id ON sync_jobs (user_id);              -- ListByUser
CREATE INDEX idx_sync_jobs_connection_id ON sync_jobs (connection_id);  -- 按源查任务
