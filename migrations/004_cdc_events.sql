-- CDC 原始事件表
CREATE TABLE IF NOT EXISTS cdc_events (
    id           BIGSERIAL PRIMARY KEY,

    -- 事件自身 WAL 位点
    lsn          TEXT        NOT NULL,  

    -- 所属事务提交位点（reader 按 Commit 边界处理后再填充；用于断点推进）
    commit_lsn   TEXT,    

    -- INSERT / UPDATE / DELETE                     
    operation    TEXT        NOT NULL,  

    -- 源 schema（如 public）        
    schema_name  TEXT        NOT NULL,    

    -- 源表（如 documents）      
    table_name   TEXT        NOT NULL,        

    -- 列值数组 [{name,value,null}]  
    payload      JSONB       NOT NULL DEFAULT '[]',  

    -- dispatcher 分发成功后标记, NULL = 未分发
    processed_at TIMESTAMPTZ,         

    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- dispatcher 按 id 顺序扫未处理行（保证 FIFO）
CREATE INDEX IF NOT EXISTS idx_cdc_events_unprocessed
    ON cdc_events (id) WHERE processed_at IS NULL;

-- 同一 WAL 位点只入库一次：崩溃后重放同一段 WAL 时靠它去重（at-least-once 的兜底）
CREATE UNIQUE INDEX IF NOT EXISTS idx_cdc_events_lsn ON cdc_events (lsn);

-- 断点位点表（SyncGuard 的 cursor 换成 LSN）
CREATE TABLE IF NOT EXISTS cdc_checkpoints (
    slot_name     TEXT PRIMARY KEY,

    -- 已确认消费成功的 LSN
    confirmed_lsn TEXT        NOT NULL,
    
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
