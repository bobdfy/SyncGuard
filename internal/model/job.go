package model

import "time"

const (
	JobStatusPending   = "pending"
	JobStatusRunning   = "running"
	JobStatusCompleted = "completed"
	JobStatusFailed    = "failed"
)

type SyncJob struct {
	ID                 int        `json:"id"`
	UserID             int        `json:"user_id"`
	ConnectionID       int        `json:"connection_id"`
	TargetConnectionID int        `json:"target_connection_id"` //目标端
	TaskName           string     `json:"task_name"`
	SyncContent        string     `json:"sync_content"` //同步内容
	Status             string     `json:"status"`
	Cursor             string     `json:"cursor"` //断点游标
	TotalCount         int        `json:"total_count"`
	ErrorMsg           string     `json:"error_msg"` //失败原因
	StartedAt          time.Time  `json:"started_at"`
	FinishedAt         *time.Time `json:"finished_at"`
}
