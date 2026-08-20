package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/bobdfy/syncguard/internal/model"
)

type JobStore struct {
	db *DB
}

func NewJobStore(db *DB) *JobStore {
	return &JobStore{db: db}
}

// Create 创建一条 sync_jobs 记录，返回自增 ID
func (s *JobStore) Create(ctx context.Context, userID int, connectionID int, targetConnectionID *int, taskName string, syncContent string) (int, error) {
	var jobID int
	err := s.db.Pool().QueryRow(ctx,
		`INSERT INTO sync_jobs(user_id, connection_id, target_connection_id, task_name, sync_content, status)
		 VALUES($1, $2, $3, $4, $5, 'pending') RETURNING id`,
		userID, connectionID, targetConnectionID, taskName, syncContent,
	).Scan(&jobID)
	if err != nil {
		return 0, fmt.Errorf("JobStore.Create: %w", err)
	}
	return jobID, nil
}

// GetByID 查单条任务
func (s *JobStore) GetByID(ctx context.Context, jobID int) (*model.SyncJob, error) {
	var j model.SyncJob
	err := s.db.Pool().QueryRow(ctx,
		`SELECT id, user_id, connection_id, target_connection_id, task_name, sync_content, status, cursor, total_count, error_msg, started_at, finished_at
		 FROM sync_jobs WHERE id = $1`, jobID,
	).Scan(&j.ID, &j.UserID, &j.ConnectionID, &j.TargetConnectionID, &j.TaskName, &j.SyncContent, &j.Status, &j.Cursor, &j.TotalCount, &j.ErrorMsg, &j.StartedAt, &j.FinishedAt)
	if err != nil {
		return nil, fmt.Errorf("JobStore.GetByID: %w", err)
	}
	return &j, nil
}

// UpdateStatus 更新任务状态
func (s *JobStore) UpdateStatus(ctx context.Context, jobID int, status string, totalCount int, cursor string, errorMsg string) error {
	_, err := s.db.Pool().Exec(ctx,
		`UPDATE sync_jobs SET status=$2, total_count=$3, cursor=$4, error_msg=$5,
		 finished_at=CASE WHEN $6 IN ('completed','failed') THEN $7 ELSE finished_at END
		 WHERE id=$1`,
		jobID, status, totalCount, cursor, errorMsg, status, time.Now(),
	)
	if err != nil {
		return fmt.Errorf("JobStore.UpdateStatus: %w", err)
	}
	return nil
}

// ListByUser 查某用户的历史任务列表
func (s *JobStore) ListByUser(ctx context.Context, userID int) ([]model.SyncJob, error) {
	rows, err := s.db.Pool().Query(ctx,
		`SELECT id, user_id, connection_id, target_connection_id, task_name, sync_content, status, cursor, total_count, error_msg, started_at, finished_at
		 FROM sync_jobs WHERE user_id = $1 ORDER BY started_at DESC`, userID,
	)
	if err != nil {
		return nil, fmt.Errorf("JobStore.ListByUser: %w", err)
	}
	defer rows.Close()

	var jobs []model.SyncJob
	for rows.Next() {
		var j model.SyncJob
		if err := rows.Scan(&j.ID, &j.UserID, &j.ConnectionID, &j.TargetConnectionID, &j.TaskName, &j.SyncContent, &j.Status, &j.Cursor, &j.TotalCount, &j.ErrorMsg, &j.StartedAt, &j.FinishedAt); err != nil {
			return nil, fmt.Errorf("JobStore.ListByUser scan: %w", err)
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("JobStore ListByUser rows: %w", err)
	}
	return jobs, nil
}

// Delete 删除一条同步任务
// WHERE id=$? AND user_id=$? —— 防止用户 A 删了用户 B 的任务
func (s *JobStore) Delete(ctx context.Context, id int, userID int) error {
	_, err := s.db.Pool().Exec(ctx,
		`DELETE FROM sync_jobs WHERE id=$1 AND user_id=$2`,
		id, userID,
	)
	if err != nil {
		return fmt.Errorf("JobStore.Delete: %w", err)
	}
	return nil
}
