package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/bobdfy/syncguard/internal/model"
)

// SyncedStore 基于 PostgreSQL 的数据存储
//
// 负责 synced_records 表的读写，实现 engine.Destination 的 Save 能力。
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

// SaveWithUser 带用户与数据源归属的批量 upsert：写入时记录 user_id、connection_id，
// 供查询时按用户/数据源过滤（对账用 connection_id 隔离多源）。
//
// 原子性：整个批次包在一个事务里，要么全部成功、要么全部失败，
// 不会留下半页数据（与 engine.Destination.Save 的契约一致）。
func (s *SyncedStore) SaveWithUser(ctx context.Context, userID, connectionID int, records []model.Record) error {
	if len(records) == 0 {
		return nil
	}

	tx, err := s.db.Pool().Begin(ctx)
	if err != nil {
		return fmt.Errorf("SaveWithUser Begin: %w", err)
	}
	defer tx.Rollback(ctx) // Commit 成功后是 no-op

	batch := &pgx.Batch{}
	for _, rec := range records {
		batch.Queue(
			`INSERT INTO synced_records (id, version, updated_at, data, user_id, connection_id)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (connection_id, id) DO UPDATE SET
			version = EXCLUDED.version,
			updated_at = EXCLUDED.updated_at,
			data = EXCLUDED.data
			`, rec.ID, rec.Version, rec.UpdatedAt, rec.Data, userID, connectionID,
		)
	}
	br := tx.SendBatch(ctx, batch)
	for range records {
		if _, err := br.Exec(); err != nil {
			_ = br.Close()
			return fmt.Errorf("SaveWithUser batch exec: %w", err)
		}
	}
	if err := br.Close(); err != nil {
		return fmt.Errorf("SaveWithUser batch close: %w", err)
	}
	return tx.Commit(ctx)
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
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ListRecordsByUser rows: %w", err)
	}
	return records, nil
}

// ListRecordsByUserAndConnection 按用户 + 数据源过滤，供对账引擎精确对比单个 connection 的数据。
func (s *SyncedStore) ListRecordsByUserAndConnection(ctx context.Context, userID, connectionID, limit, offset int) ([]model.Record, error) {
	rows, err := s.db.Pool().Query(ctx,
		`SELECT id, version, updated_at, data
		 FROM synced_records WHERE user_id = $1 AND connection_id = $2 ORDER BY id LIMIT $3 OFFSET $4`,
		userID, connectionID, limit, offset,
	)
	if err != nil {
		return nil, fmt.Errorf("ListRecordsByUserAndConnection: %w", err)
	}
	defer rows.Close()

	var records []model.Record
	for rows.Next() {
		var r model.Record
		if err := rows.Scan(&r.ID, &r.Version, &r.UpdatedAt, &r.Data); err != nil {
			return nil, fmt.Errorf("ListRecordsByUserAndConnection scan: %w", err)
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ListRecordsByUserAndConnection rows: %w", err)
	}
	return records, nil
}
