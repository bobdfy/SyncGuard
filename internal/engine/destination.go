package engine

import (
	"context"

	"github.com/bobdfy/syncguard/internal/model"
)

// Destination 数据目的地接口
//
// 同步引擎把数据从 Source 拉出来后，通过这个接口写入目标端。
// V0 阶段只有一个实现：repository.SyncedStore（写 PostgreSQL）。
// 后期切换目标（Redis、文件、外部 API）只需新增实现，引擎不动。
type Destination interface {
	// Save 批量保存记录
	//
	// 语义：有则更新（ON CONFLICT），无则插入。
	// 幂等：同一条记录写两次，结果一致。
	// 传入一批记录，要么全部成功，要么全部失败（原子性由 DB 事务保证）。
	Save(ctx context.Context, records []model.Record) error

	// GetCheckpoint 读取断点游标
	//
	// taskName 区分不同同步任务（如 "mock_sync"、"github_sync"）。
	// 返回游标值；首次运行时 last_cursor = ""，由调用方判断。
	// 查询不到记录时返回空字符串（不是错误）。
	GetCheckpoint(ctx context.Context, taskName string) (lastCursor string, err error)

	// UpdateCheckpoint 更新断点游标
	//
	// 每同步完一页调用一次，记录当前分页的最后一条记录 ID。
	// 用 INSERT … ON CONFLICT (task_name) DO UPDATE 实现 upsert。
	UpdateCheckpoint(ctx context.Context, taskName string, cursor string) error

	// CreateBatch 创建同步批次
	//
	// 引擎启动时调用，插入一条 status='running' 的记录。
	// 返回自增批次 ID，用于后续 CompleteBatch。
	CreateBatch(ctx context.Context) (batchID int64, err error)

	// CompleteBatch 标记批次完成
	//
	// 引擎同步结束（成功或失败）时调用。
	// 更新 status、finished_at、total_count。
	CompleteBatch(ctx context.Context, batchID int64, status string, totalCount int) error
}