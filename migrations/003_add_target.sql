-- V1 同步任务扩展：目标源 + 同步内容
ALTER TABLE sync_jobs
    ADD COLUMN IF NOT EXISTS target_connection_id INT REFERENCES connections(id),
    ADD COLUMN IF NOT EXISTS sync_content TEXT DEFAULT '';