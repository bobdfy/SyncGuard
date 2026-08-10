package mq

import "time"

// JobMessage 发布到 RabbitMQ 的 JSON 消息
// 定义：Producer 发给 RabbitMQ 的消息长什么样
type JobMessage struct {
	JobID     int       `json:"job_id"`
	TaskName  string    `json:"task_name"`
	Timestamp time.Time `json:"timestamp"` //记录 Producer 创建并发送这条任务消息的时间。
}
