package postgres

import (
	"context"
	"fmt"
	"regexp"

	"github.com/bobdfy/syncguard/internal/engine"
	"github.com/bobdfy/syncguard/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// 编译期检查：Destination 实现了 engine.Destination 接口
var _ engine.Destination = (*Destination)(nil)

// identRe 合法标识符白名单（防注入，和 source/postgres 同款正则）。
var identRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// Destination 把同步数据写入外部 PostgreSQL 的一张标准表。
// 目标表结构约定：id TEXT PK, version INT, updated_at TIMESTAMPTZ, data JSONB。
type Destination struct {
	dbURL string
	table string
	pool  *pgxpool.Pool
}

// NewDestination 校验表名 + 建立到目标库的连接池。
func NewDestination(ctx context.Context, dbURL, table string) (*Destination, error) {
	if !identRe.MatchString(table) {
		return nil, fmt.Errorf("非法目标表名 %q（只允许字母/数字/下划线）", table)
	}
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return nil, fmt.Errorf("连接目标库失败: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("连接目标库失败: %w", err)
	}
	return &Destination{dbURL: dbURL, table: table, pool: pool}, nil
}

// Close 释放连接池。调用方（worker）用完目标端后必须 defer 关闭，
// 否则每次同步到外部 PG 目标都会泄漏一个连接池。
func (d *Destination) Close() error {
	if d.pool != nil {
		d.pool.Close()
	}
	return nil
}

// Save 批量 upsert 记录到目标表（幂等 + 原子）。
//
// 原子性：整个批次包在一个事务里，要么全成功、要么全失败，
// 不会留下半页数据（与 engine.Destination.Save 的契约一致）。
func (d *Destination) Save(ctx context.Context, records []model.Record) error {
	if len(records) == 0 {
		return nil
	}
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("Destination.Save Begin: %w", err)
	}
	defer tx.Rollback(ctx) // Commit 成功后是 no-op

	batch := &pgx.Batch{}
	for _, rec := range records {
		batch.Queue(
			`INSERT INTO `+d.table+` (id, version, updated_at, data)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (id) DO UPDATE SET
			version = EXCLUDED.version,
			updated_at = EXCLUDED.updated_at,
			data = EXCLUDED.data
			`, rec.ID, rec.Version, rec.UpdatedAt, rec.Data,
		)
	}
	br := tx.SendBatch(ctx, batch)
	for range records {
		if _, err := br.Exec(); err != nil {
			_ = br.Close()
			return fmt.Errorf("Destination.Save batch exec: %w", err)
		}
	}
	if err := br.Close(); err != nil {
		return fmt.Errorf("Destination.Save batch close: %w", err)
	}
	return tx.Commit(ctx)
}

// GetCheckpoint 空实现：断点由元数据库管理，外部目标不保存断点。
func (d *Destination) GetCheckpoint(ctx context.Context, taskName string) (string, error) {
	return "", nil
}

// UpdateCheckpoint 空实现。
func (d *Destination) UpdateCheckpoint(ctx context.Context, taskName string, cursor string) error {
	return nil
}

// CreateBatch 空实现。
func (d *Destination) CreateBatch(ctx context.Context) (int64, error) {
	return 0, nil
}

// CompleteBatch 空实现。
func (d *Destination) CompleteBatch(ctx context.Context, batchID int64, status string, totalCount int) error {
	return nil
}
