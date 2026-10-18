-- 1. users — 用户
CREATE TABLE users (
    id            SERIAL PRIMARY KEY,
    username      VARCHAR NOT NULL UNIQUE,
    password_hash VARCHAR NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. connections — 数据源连接（每个连接归属一个用户）
--    cdc_enabled / target_url：CDC 多租户开关与镜像目标（每连接一份）。
CREATE TABLE connections (
    id          SERIAL PRIMARY KEY,
    user_id     INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name        VARCHAR NOT NULL,
    source_type VARCHAR NOT NULL,   -- mock / github / postgres ...
    source_url  VARCHAR NOT NULL,
    cdc_enabled BOOLEAN NOT NULL DEFAULT false,
    target_url  VARCHAR NOT NULL DEFAULT '',
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
--    复合主键 (connection_id, id)：同一数据源的同一条记录，在不同 connection
--    （不同用户 / 同一用户多个源连接）下各自独立，互不覆盖。
--    user_id 是冗余列（等价于 connections.user_id），仅为查询免 JOIN。
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

-- 5. outbox — 普通同步链路的事务发件箱（dispatcher 搬运 + worker 延迟重投）
CREATE TABLE outbox (
    id         BIGSERIAL PRIMARY KEY,
    payload    JSONB NOT NULL,           -- 要发的消息（JobMessage 的 JSON）
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at    TIMESTAMPTZ               -- NULL = 未发送
);

-- 6. cdc_events — CDC 事件发件箱（reader 落库 → dispatcher 投 MQ → writer 镜像）
--    (connection_id, lsn) 是去重键：LSN 是每个 PG 实例自己的坐标系，不同实例会撞值，
--    必须加连接维度才能唯一；崩溃重放靠 ON CONFLICT (connection_id, lsn) DO NOTHING 兜底。
CREATE TABLE cdc_events (
    id            BIGSERIAL PRIMARY KEY,
    connection_id INT         NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
    lsn           TEXT        NOT NULL,   -- 事件自身 WAL 位点
    commit_lsn    TEXT,                    -- 所属事务提交位点（reader 按 commit 边界填充）
    operation     TEXT        NOT NULL,   -- INSERT / UPDATE / DELETE
    schema_name   TEXT        NOT NULL,   -- 源 schema（如 public）
    table_name    TEXT        NOT NULL,   -- 源表（如 documents）
    payload       JSONB       NOT NULL DEFAULT '[]',  -- {"columns":[...],"relation":{...}}
    processed_at  TIMESTAMPTZ,            -- dispatcher 分发成功后标记，NULL = 未分发
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 7. cdc_checkpoints — CDC 断点（SyncGuard 的 cursor 换成 LSN）
--    主键 (connection_id, slot_name)：slot_name 跨实例可重复，须与连接一起才唯一。
CREATE TABLE cdc_checkpoints (
    connection_id INT         NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
    slot_name     TEXT        NOT NULL,
    confirmed_lsn TEXT        NOT NULL,   -- 已确认消费成功的 LSN
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (connection_id, slot_name)
);

-- 索引：覆盖代码里的实际查询路径
CREATE INDEX idx_synced_records_user_id ON synced_records (user_id);    -- ListRecordsByUser
CREATE INDEX idx_sync_jobs_user_id       ON sync_jobs (user_id);        -- ListByUser
CREATE INDEX idx_sync_jobs_connection_id ON sync_jobs (connection_id);  -- 按源查任务

-- dispatcher 按 id 顺序扫未处理行（保证 FIFO）
CREATE INDEX idx_cdc_events_unprocessed
    ON cdc_events (id) WHERE processed_at IS NULL;

-- 同一连接内 WAL 位点只入库一次（Append 的 ON CONFLICT 去重目标）
CREATE UNIQUE INDEX idx_cdc_events_conn_lsn ON cdc_events (connection_id, lsn);
