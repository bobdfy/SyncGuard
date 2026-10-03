package repository

import (
	"context"
	"fmt"

	"github.com/bobdfy/syncguard/internal/engine"
)

// 编译期检查：JobProgress 实现了 engine.Progress 接口
var _ engine.Progress = (*JobProgress)(nil)

// JobProgress 同步进度/断点的最小实现：只读写 sync_jobs 表，不做数据落地。
//
// 镜像轨道只需「进度 + 断点」（数据写入由 TableDestination 负责，走 SaveRows 而非信封 Save），
// 因此从 JobDestination 里把进度部分抽出来单独成类型，避免给镜像轨道硬塞一个无意义的 store。
// 逻辑与 JobDestination 的 4 个进度方法保持一致。
type JobProgress struct {
	jobStore   *JobStore
	jobID      int
	totalCount int
	cursor     string
}

// NewJobProgress 创建进度对象。
func NewJobProgress(jobStore *JobStore, jobID int) *JobProgress {
	return &JobProgress{jobStore: jobStore, jobID: jobID}
}

// GetCheckpoint 从 sync_jobs 表读取断点游标，并缓存上次进度。
func (p *JobProgress) GetCheckpoint(ctx context.Context, taskName string) (string, error) {
	job, err := p.jobStore.GetByID(ctx, p.jobID)
	if err != nil {
		return "", fmt.Errorf("GetCheckpoint: %w", err)
	}
	p.totalCount = job.TotalCount
	p.cursor = job.Cursor
	return job.Cursor, nil
}

// UpdateCheckpoint 更新 sync_jobs 的 cursor 字段。
func (p *JobProgress) UpdateCheckpoint(ctx context.Context, taskName string, cursor string) error {
	return p.jobStore.UpdateStatus(ctx, p.jobID, "running", p.totalCount, cursor, "")
}

// CreateBatch 标记任务开始执行，返回 jobID 作为批次 ID。
func (p *JobProgress) CreateBatch(ctx context.Context) (int64, error) {
	err := p.jobStore.UpdateStatus(ctx, p.jobID, "running", p.totalCount, p.cursor, "")
	if err != nil {
		return 0, fmt.Errorf("CreateBatch: %w", err)
	}
	return int64(p.jobID), nil
}

// CompleteBatch 标记任务结束，status 由引擎透传（completed/failed）。
func (p *JobProgress) CompleteBatch(ctx context.Context, batchID int64, status string, totalCount int) error {
	return p.jobStore.UpdateStatus(ctx, p.jobID, status, totalCount, "", "")
}
