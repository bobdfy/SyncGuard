package mq

import (
	"encoding/json"
	"time"
)

// JobMessage 发布到 RabbitMQ 的 JSON 消息。
type JobMessage struct {
	JobID        int       `json:"job_id"`
	TaskName     string    `json:"task_name"`
	Timestamp    time.Time `json:"timestamp"`     // Producer 创建这条消息的时间
	ConnectionID int       `json:"connection_id"` // 数据源连接 ID
	Attempt      int       `json:"attempt"`       // 第几次投递（0=首次），阶段4 指数退避用
}

// OutboxPayload outbox 表里存的消息内容：任务消息字段 + 延迟毫秒。
// DelayMs = 0 表示立即发送，>0 表示延迟该毫秒数后发送（由搬运工决定走哪条发送路径）。
type OutboxPayload struct {
	JobID        int    `json:"job_id"`
	TaskName     string `json:"task_name"`
	ConnectionID int    `json:"connection_id"`
	Attempt      int    `json:"attempt"`
	DelayMs      int64  `json:"delay_ms"`
}

// MarshalOutboxPayload 把任务信息 + 延迟序列化成 outbox 的 payload。
// 供双写点（RunJob / 重投）写入 outbox 表使用。
func MarshalOutboxPayload(jobID int, taskName string, connectionID int, attempt int, delayMs int64) ([]byte, error) {
	p := OutboxPayload{
		JobID:        jobID,
		TaskName:     taskName,
		ConnectionID: connectionID,
		Attempt:      attempt,
		DelayMs:      delayMs,
	}
	return json.Marshal(p)
}
