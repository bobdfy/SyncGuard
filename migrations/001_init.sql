-- V0 初始化建表
-- 三张表支撑断点续传：中断重启后不丢数据

-- ============================================================
-- 1. synced_records — 同步记录落地存储
-- ============================================================
-- 用源端 ID 做主键，重复同步时 ON CONFLICT 更新
-- data 用 JSONB：支持后续对 JSON 字段建索引，JSON 不行
CREATE TABLE IF NOT EXISTS synced_records (
    id              TEXT        PRIMARY KEY,           -- 源端记录 ID（全局唯一）
    version         INTEGER     NOT NULL,              -- 当前版本号，每次更新 +1
    updated_at      TIMESTAMPTZ NOT NULL,              -- 源端最后修改时间
    data            JSONB       NOT NULL DEFAULT '{}', -- 原始 JSON 载荷
    synced_at       TIMESTAMPTZ NOT NULL DEFAULT NOW() -- 本系统入库时间
);

-- ============================================================
-- 2. sync_batches — 同步批次日志
-- ============================================================
-- 每次引擎启动执行一次同步算一个 batch
-- status: 'running' → 开始同步时写入，结束时更新为 'completed' 或 'failed'
CREATE TABLE IF NOT EXISTS sync_batches (
    id          BIGSERIAL    PRIMARY KEY,              -- 自增批次 ID
    status      TEXT         NOT NULL DEFAULT 'running', -- running / completed / failed
    started_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),   -- 批次开始时间
    finished_at TIMESTAMPTZ,                           -- 批次结束时间（完成或失败时写入）
    total_count INTEGER      NOT NULL DEFAULT 0        -- 本批次共同步了多少条
);

-- ============================================================
-- 3. sync_checkpoints — 断点游标
-- ============================================================
-- 每个同步任务一行，记录"上一次同步到哪了"
-- 重启后读 last_cursor，从这里继续，cursor = '' 表示从头开始
-- 引擎每同步完一页就 UPDATE 这里，保证崩了之后最多丢一页
CREATE TABLE IF NOT EXISTS sync_checkpoints (
    task_name    TEXT        PRIMARY KEY,              -- 任务名称（如 'mock_sync'）
    last_cursor  TEXT        NOT NULL DEFAULT '',      -- 上次完成位置的游标
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()    -- 最后更新时间
);