package engine

import (
	"context"
	"fmt"
	"log"

	"github.com/bobdfy/syncguard/internal/model"
)

// TableSource 数据库镜像源的能力接口。
//
// 数据库源（如 postgres.Source）除了满足 Source（信封轨道），还额外实现本接口，
// 用于「原表结构镜像」轨道：读源表结构 + 取原生行。调用方用类型断言探测该能力。
type TableSource interface {
	// TableSchema 返回源表结构（列名 + 类型 + 主键）。
	TableSchema(ctx context.Context) (model.TableSchema, error)

	// FetchRows 按主键 keyset 分页取原生行；cursor 为空表示第一页。
	FetchRows(ctx context.Context, cursor string, limit int) ([]model.RawRow, string, bool, error)

	Close() error
}

// TableDestination 数据库镜像目标的能力接口。
type TableDestination interface {
	// EnsureTable 按源表结构在目标库建同构表（幂等，CREATE TABLE IF NOT EXISTS）。
	EnsureTable(ctx context.Context, schema model.TableSchema) error

	// SaveRows 列对列批量写入原生行（事务内原子，ON CONFLICT 幂等覆盖）。
	SaveRows(ctx context.Context, rows []model.RawRow) error

	Close() error
}

// Progress 同步进度/断点的最小接口。
//
// 从 Destination 的 4 个进度方法抽出；repository.JobProgress 与 JobDestination 都满足它。
// 镜像轨道的断点仍写 sync_jobs，与信封轨道共用同一套进度表，只是数据落点不同。
type Progress interface {
	GetCheckpoint(ctx context.Context, taskName string) (string, error)
	UpdateCheckpoint(ctx context.Context, taskName string, cursor string) error
	CreateBatch(ctx context.Context) (int64, error)
	CompleteBatch(ctx context.Context, batchID int64, status string, totalCount int) error
}

// RunTable 镜像轨道的同步循环：读结构 → 建表 → 分页列对列写入 → 更新断点。
//
// 与 Run（信封轨道）平行，复用相同的断点/失败收尾语义（每页写完更新游标、崩溃最多丢一页、
// 失败时 CompleteBatch("failed")）。
func RunTable(ctx context.Context, src TableSource, dst TableDestination, progress Progress, taskName string, pageSize int) error {
	// 1. 读源表结构 + 目标建同构表
	schema, err := src.TableSchema(ctx)
	if err != nil {
		return fmt.Errorf("TableSchema: %w", err)
	}
	if err := dst.EnsureTable(ctx, schema); err != nil {
		return fmt.Errorf("EnsureTable: %w", err)
	}

	// 2. 读断点 + 建批次
	cursor, err := progress.GetCheckpoint(ctx, taskName)
	if err != nil {
		return fmt.Errorf("GetCheckpoint: %w", err)
	}
	batchID, err := progress.CreateBatch(ctx)
	if err != nil {
		return fmt.Errorf("CreateBatch: %w", err)
	}

	totalCount := 0
	ok := false
	defer func() {
		if !ok {
			_ = progress.CompleteBatch(context.Background(), batchID, "failed", totalCount)
		}
	}()

	// 3. 分页循环
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rows, nextCursor, hasMore, err := src.FetchRows(ctx, cursor, pageSize)
		if err != nil {
			return fmt.Errorf("FetchRows: %w", err)
		}

		if len(rows) > 0 {
			if err := dst.SaveRows(ctx, rows); err != nil {
				return fmt.Errorf("SaveRows: %w", err)
			}
			totalCount += len(rows)
		}

		if !hasMore {
			break
		}
		if err := progress.UpdateCheckpoint(ctx, taskName, nextCursor); err != nil {
			return fmt.Errorf("UpdateCheckpoint: %w", err)
		}
		cursor = nextCursor
	}

	if err := progress.CompleteBatch(ctx, batchID, "completed", totalCount); err != nil {
		return fmt.Errorf("CompleteBatch: %w", err)
	}
	ok = true
	log.Printf("镜像同步完成: task=%s, total=%d", taskName, totalCount)
	return nil
}
