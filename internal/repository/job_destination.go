package repository

import (
	"context"
	"fmt"

	"github.com/bobdfy/syncguard/internal/engine"
	"github.com/bobdfy/syncguard/internal/model"
)

// 编译期检查：确保 JobDestination 实现了 engine.Destination 接口
var _ engine.Destination = (*JobDestination)(nil)

// JobDestination 把引擎进度写进 sync_jobs，代替 V0 的 checkpoints / batches 表
type JobDestination struct {
	store      *SyncedStore // 负责 Save（写同步数据到目标库）
	jobStore   *JobStore    // 负责读写 sync_jobs 表
	jobID      int          // 当前任务的 ID
	userID     int
	totalCount int    // 内存里累加，避免每次查 DB
	cursor     string // 断点游标：GetCheckpoint 时缓存，避免 CreateBatch 清零
}

func NewJobDestination(store *SyncedStore, jobStore *JobStore, jobID int, userID int) *JobDestination {
	return &JobDestination{
		store:    store,
		jobStore: jobStore,
		jobID:    jobID,
		userID:   userID,
	}
}

// ========== 以下是 engine.Destination 接口的四个方法 ==========

// Save 把同步数据写入目标库，然后更新 sync_jobs 的 total_count
//
// 引擎每拉完一页数据就调一次 Save。
// 1. 委托 SyncedStore.Save 写数据到目标库（幂等：ON CONFLICT DO UPDATE）
// 2. 累加本页条数到内存 totalCount
// 3. 调 UpdateStatus 把最新的 totalCount 写回 sync_jobs，前端轮询就能看到进度
func (d *JobDestination) Save(ctx context.Context, records []model.Record) error {
	// 1. 写数据到目标库
	if err := d.store.SaveWithUser(ctx, d.userID, records); err != nil {
		return fmt.Errorf("JobDestination.Save: %w", err)
	}
	// 2. 累加本页条数
	d.totalCount += len(records)

	// 3. 更新 sync_jobs：状态="running"，total_count=最新值
	//    UpdateStatus(ctx, jobID, 状态, totalCount, cursor, errorMsg)
	return d.jobStore.UpdateStatus(ctx, d.jobID, "running", d.totalCount, "", "")
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
//
// UpdateStatus(ctx, jobID, 状态, totalCount, cursor, errorMsg)
// 状态还是 running（同步还没结束），cursor 用参数里的值
func (d *JobDestination) UpdateCheckpoint(ctx context.Context, taskName string, cursor string) error {
	return d.jobStore.UpdateStatus(ctx, d.jobID, "running", d.totalCount, cursor, "")
}

// CreateBatch 标记任务开始执行
//
// 引擎启动时调用。
// 把 sync_jobs 状态改为 "running"，通知前端任务已开始。
// StartAt 由 UpdateStatus 或 DB 层处理。
//
// 返回值是批次 ID，这里直接用 jobID
func (d *JobDestination) CreateBatch(ctx context.Context) (int64, error) {
	// 状态改为 running，但 total_count / cursor 沿用 GetCheckpoint 缓存的旧值，
	// 不清零，保证崩溃重启后能从断点继续、进度不丢。
	err := d.jobStore.UpdateStatus(ctx, d.jobID, "running", d.totalCount, d.cursor, "")
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
//
// 参数 status 是引擎传进来的，直接透传即可，
// 不要写死成 "completed"——引擎失败时传的是 "failed"。
func (d *JobDestination) CompleteBatch(ctx context.Context, batchID int64, status string, totalCount int) error {
	// UpdateStatus(ctx, jobID, 状态, totalCount, cursor, errorMsg)
	// 状态用参数 status，不要硬编码
	return d.jobStore.UpdateStatus(ctx, d.jobID, status, totalCount, "", "")
}
