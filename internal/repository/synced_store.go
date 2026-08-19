package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/bobdfy/syncguard/internal/engine"
	"github.com/bobdfy/syncguard/internal/model"
)

// 编译期检查：SyncedStore 实现了 engine.Destination 接口
var _ engine.Destination = (*SyncedStore)(nil)

// SyncedStore 基于 PostgreSQL 的数据存储
//
// 实现 engine.Destination 接口，负责所有 SQL 操作。
// 依赖注入 DB 连接池，不自己创建连接。
type SyncedStore struct {
	db *DB
}

// NewSyncedStore 构造函数
//
// db 不能为 nil，调用方保证已通过 NewDB 创建并 Ping 成功。
func NewSyncedStore(db *DB) *SyncedStore {
	return &SyncedStore{db: db}
}

// Save 批量 upsert 记录
//
// 用 pgx.Batch 把多条 SQL 打包成一个网络往返：
//  1. 遍历 records，每条构造 INSERT … ON CONFLICT … DO UPDATE
//     - 冲突时覆盖 version、updated_at、data
//  2. db.Pool().SendBatch(ctx, batch) 一次性发送
//  3. 逐个 br.Exec() 确认每条都成功
//  4. 任何一条失败 → 返回 error
func (s *SyncedStore) Save(ctx context.Context, records []model.Record) error {
	if len(records) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, rec := range records {
		batch.Queue(
			`INSERT INTO synced_records (id, version, updated_at, data) 
			VALUES ($1, $2, $3, $4) 
			ON CONFLICT (id) DO UPDATE SET
			version = EXCLUDED.version,
			updated_at = EXCLUDED.updated_at,
			data = EXCLUDED.data
			`, rec.ID, rec.Version, rec.UpdatedAt, rec.Data,
		)
	}
	br := s.db.Pool().SendBatch(ctx, batch)
	defer br.Close()
	for range records {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("Save batch exec: %w", err)
		}
	}
	return nil
}

// SaveWithUser 带用户归属的批量 upsert：写入时记录 user_id，供查询时按用户过滤。
func (s *SyncedStore) SaveWithUser(ctx context.Context, userID int, records []model.Record) error {
	if len(records) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, rec := range records {
		batch.Queue(
			`INSERT INTO synced_records (id, version, updated_at, data, user_id) 
			VALUES ($1, $2, $3, $4, $5) 
			ON CONFLICT (id) DO UPDATE SET
			version = EXCLUDED.version,
			updated_at = EXCLUDED.updated_at,
			data = EXCLUDED.data
			`, rec.ID, rec.Version, rec.UpdatedAt, rec.Data, userID,
		)
	}
	br := s.db.Pool().SendBatch(ctx, batch)
	defer br.Close()
	for range records {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("SaveWithUser batch exec: %w", err)
		}
	}
	return nil
}

// GetCheckpoint 读取断点游标
//
// 查询 sync_checkpoints 表，找不到返回空字符串不报错。
//  1. SELECT last_cursor FROM sync_checkpoints WHERE task_name = $1
//  2. 用 pgx.ErrNoRows 判断无结果 → 返回 "", nil
//  3. 其他错误透传
func (s *SyncedStore) GetCheckpoint(ctx context.Context, taskName string) (string, error) {
	var cursor string
	err := s.db.Pool().QueryRow(ctx, `SELECT last_cursor FROM sync_checkpoints WHERE task_name = $1`, taskName).Scan(&cursor)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("GetCheckpoint: %w", err)
	}
	return cursor, nil
}

// UpdateCheckpoint 更新断点游标
//
// UPSERT 一条 checkpoints 记录：
//  1. INSERT INTO sync_checkpoints (task_name, last_cursor) VALUES ($1, $2)
//  2. ON CONFLICT (task_name) DO UPDATE SET last_cursor = EXCLUDED.last_cursor, updated_at = NOW()
//  3. 首次调用 task_name 不存在时 INSERT，后续调用 UPDATE
func (s *SyncedStore) UpdateCheckpoint(ctx context.Context, taskName string, cursor string) error {
	_, err := s.db.Pool().Exec(ctx,
		`INSERT INTO sync_checkpoints (task_name, last_cursor, updated_at)
	VALUES ($1, $2, Now())
	ON CONFLICT (task_name) DO UPDATE SET last_cursor = EXCLUDED.last_cursor,
	updated_at = NOW()`,
		taskName, cursor)
	if err != nil {
		return fmt.Errorf("UpdateCheckpoint: %w", err)
	}
	return nil
}

// CreateBatch 创建同步批次
//
// 插入一条 status='running' 的批次记录，返回自增 ID：
//  1. INSERT INTO sync_batches (status) VALUES ('running') RETURNING id
//  2. 用 QueryRow + Scan 拿回 batchID
func (s *SyncedStore) CreateBatch(ctx context.Context) (int64, error) {
	var batchID int64
	err := s.db.Pool().QueryRow(ctx, `INSERT INTO sync_batches (status) VALUES ('running') RETURNING id`).Scan(&batchID)
	if err != nil {
		return 0, fmt.Errorf("CreateBatch: %w", err)
	}
	return batchID, nil
}

// CompleteBatch 标记批次完成
//
// 同步结束时调用，更新批次记录：
//  1. UPDATE sync_batches SET status=$2, finished_at=NOW(), total_count=$3 WHERE id=$1
//  2. status 传 "completed" 或 "failed"，由调用方决定
func (s *SyncedStore) CompleteBatch(ctx context.Context, batchID int64, status string, totalCount int) error {
	_, err := s.db.Pool().Exec(ctx, `UPDATE sync_batches SET status = $2, finished_at = NOW(), total_count = $3 WHERE id = $1`, batchID, status, totalCount)
	if err != nil {
		return fmt.Errorf("CompleteBatch: %w", err)
	}
	return nil
}

// ListRecords 查询 synced_records 表，返回已同步的记录
// limit 控制最多返回条数，按 id 升序排列
func (s *SyncedStore) ListRecords(ctx context.Context, limit, offset int) ([]model.Record, error) {
	rows, err := s.db.Pool().Query(ctx,
		`SELECT id, version, updated_at, data
		 FROM synced_records ORDER BY id LIMIT $1 OFFSET $2`,
		limit, offset,
	)
	if err != nil {
		return nil, fmt.Errorf("ListRecords: %w", err)
	}
	defer rows.Close()

	var records []model.Record
	for rows.Next() {
		var r model.Record
		if err := rows.Scan(&r.ID, &r.Version, &r.UpdatedAt, &r.Data); err != nil {
			return nil, fmt.Errorf("ListRecords scan: %w", err)
		}
		records = append(records, r)
	}
	return records, nil
}

// ListRecordsByUser 只查询某个用户同步的记录。
func (s *SyncedStore) ListRecordsByUser(ctx context.Context, userID, limit, offset int) ([]model.Record, error) {
	rows, err := s.db.Pool().Query(ctx,
		`SELECT id, version, updated_at, data
		 FROM synced_records WHERE user_id = $1 ORDER BY id LIMIT $2 OFFSET $3`,
		userID, limit, offset,
	)
	if err != nil {
		return nil, fmt.Errorf("ListRecordsByUser: %w", err)
	}
	defer rows.Close()

	var records []model.Record
	for rows.Next() {
		var r model.Record
		if err := rows.Scan(&r.ID, &r.Version, &r.UpdatedAt, &r.Data); err != nil {
			return nil, fmt.Errorf("ListRecordsByUser scan: %w", err)
		}
		records = append(records, r)
	}
	return records, nil
}
