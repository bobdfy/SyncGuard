package model

import "time"

const (
	JobStatusPending   = "pending"
	JobStatusRunning   = "running"
	JobStatusCompleted = "completed"
	JobStatusFailed    = "failed"
)

type SyncJob struct {
	ID                 int       `json:"id"`
	UserID             int       `json:"user_id"`
	ConnectionID       int       `json:"connection_id"`
	TargetConnectionID int       `json:"target_connection_id"`
	TaskName           string    `json:"task_name"`
	SyncContent        string    `json:"sync_content"`
	Status             string    `json:"status"`
	Cursor             string    `json:"cursor"`
	TotalCount         int       `json:"total_count"`
	ErrorMsg           string    `json:"error_msg"`
	StartedAt          time.Time `json:"started_at"`
	FinishedAt         *time.Time `json:"finished_at"`
}
