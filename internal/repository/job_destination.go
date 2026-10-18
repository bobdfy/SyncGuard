package repository

import (
	"context"
	"fmt"

	"github.com/bobdfy/syncguard/internal/engine"
	"github.com/bobdfy/syncguard/internal/model"
)

// 编译期检查：确保 JobDestination 实现了 engine.Destination 接口
var _ engine.Destination = (*JobDestination)(nil)

// JobDestination 把引擎进度写进 sync_jobs 表。
type JobDestination struct {
	store      engine.Destination // 真正的数据落点
	jobStore   *JobStore          // 负责读写 sync_jobs 表
	jobID      int                // 当前任务的 ID
	totalCount int                // 内存里累加，避免每次查 DB
	cursor     string             // 断点游标：GetCheckpoint 时缓存，避免 CreateBatch 清零
}

func NewJobDestination(store engine.Destination, jobStore *JobStore, jobID int) *JobDestination {
	return &JobDestination{
		store:    store,
		jobStore: jobStore,
		jobID:    jobID,
	}
}

// ========== 以下是 engine.Destination 接口的实现 ==========

// Save 把同步数据写入目标，然后更新 sync_jobs 的 total_count
//
// 引擎每拉完一页数据就调一次 Save。
func (d *JobDestination) Save(ctx context.Context, records []model.Record) error {
	// 1. 写数据到目标
	if err := d.store.Save(ctx, records); err != nil {
		return fmt.Errorf("JobDestination.Save: %w", err)
	}
	// 2. 累加本页条数
	d.totalCount += len(records)

	// 3. 更新 sync_jobs：状态="running"，total_count=最新值
	// cursor 传当前缓存值（而非 ""），避免每页 Save 清空断点游标。
	return d.jobStore.UpdateStatus(ctx, d.jobID, "running", d.totalCount, d.cursor, "")
}

// GetCheckpoint 从 sync_jobs 表读取断点游标
//
// 引擎启动时调用一次，拿到上次停在哪，从那继续。
// 首次运行 cursor=""，引擎会从头发送。
//
// 返回值类型：光标是字符串。
func (d *JobDestination) GetCheckpoint(ctx context.Context, taskName string) (string, error) {
	// 1. 查 sync_jobs 拿到整条记录
	job, err := d.jobStore.GetByID(ctx, d.jobID)
	if err != nil {
		return "", fmt.Errorf("GetCheckpoint: %w", err)
	}
	// 2. 缓存上次的进度和断点，供后续 CreateBatch 沿用，避免崩溃重启后从头再来
	d.totalCount = job.TotalCount
	d.cursor = job.Cursor
	return job.Cursor, nil
}

// UpdateCheckpoint 更新 sync_jobs 的 cursor 字段
//
// 引擎每写完一页并更新完断点后调用。
// 把当前页最后一条记录的 ID 存进 cursor，
// 万一崩溃，重启后从这继续，最多丢一页。
func (d *JobDestination) UpdateCheckpoint(ctx context.Context, taskName string, cursor string) error {
	// 先刷新内存缓存再落库：让 CompleteBatch 能拿到可信的当前游标，
	d.cursor = cursor
	return d.jobStore.UpdateStatus(ctx, d.jobID, "running", d.totalCount, cursor, "")
}

// CreateBatch 标记任务开始执行
//
// 引擎启动时调用。
// 把 sync_jobs 状态改为 "running"，通知前端任务已开始，并刷新 started_at
func (d *JobDestination) CreateBatch(ctx context.Context) (int64, error) {
	// 缓存的旧值，不清零，保证崩溃重启后能从断点继续、进度不丢。
	err := d.jobStore.MarkStarted(ctx, d.jobID, d.totalCount, d.cursor)
	if err != nil {
		return 0, fmt.Errorf("CreateBatch: %w", err)
	}
	// jobID 是 int，CreateBatch 要求返回 int64，强转一下
	return int64(d.jobID), nil
}

// CompleteBatch 标记任务结束
//
// 引擎同步结束（成功或失败）时调用。
// 把最终状态（"completed" 或 "failed"）和总数写回 sync_jobs。
func (d *JobDestination) CompleteBatch(ctx context.Context, batchID int64, status string, totalCount int) error {
	// 状态用参数 status，不要硬编码。
	// cursor 传 d.cursor 而非 ""：失败/完成后保留断点，重试才能续传而非从头再来。
	// totalCount 用 d.totalCount
	return d.jobStore.UpdateStatus(ctx, d.jobID, status, d.totalCount, d.cursor, "")
}

// Close 透传到底层 Destination，释放其资源（如外部 PG 连接池）。
func (d *JobDestination) Close() error {
	return d.store.Close()
}
