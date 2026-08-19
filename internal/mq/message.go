package mq

import "time"

// JobMessage 发布到 RabbitMQ 的 JSON 消息。
type JobMessage struct {
	JobID        int       `json:"job_id"`
	TaskName     string    `json:"task_name"`
	Timestamp    time.Time `json:"timestamp"`     // Producer 创建这条消息的时间
	ConnectionID int       `json:"connection_id"` // 数据源连接 ID
	Attempt      int       `json:"attempt"`       // 第几次投递（0=首次），阶段4 指数退避用
}
