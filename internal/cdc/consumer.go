package cdc

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/bobdfy/syncguard/internal/mq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Consumer 消费 CDC 队列的逐消息处理逻辑，相当于 jobs 侧的 worker.Handler
type Consumer struct {
	applier *Applier
}

// NewConsumer 用给定的 Applier 组装消费者
func NewConsumer(applier *Applier) *Consumer {
	return &Consumer{applier: applier}
}

// Handle 处理单条消息。成功 Ack；解析失败或重试耗尽 Nack（进死信）
func (c *Consumer) Handle(msg amqp.Delivery) {
	var ev Event
	if err := json.Unmarshal(msg.Body, &ev); err != nil {
		// 解析失败是永久错误，重试无意义，直接进死信
		slog.Error("解析 CDC 事件失败，进死信", "err", err)
		msg.Nack(false, false) // 队列配了 DLX，requeue=false 会进死信队列
		return
	}

	// 就地重试：失败后按 mq.BackoffDelay 退避再试同一条消息。
	// 原地重试是为了保序：CDC 的 INSERT/UPDATE/DELETE 有先后依赖。
	// 代价：单条坏消息会堵住整条队列最长 ~7 分钟（MaxAttempts 次 × 递增退避），
	// 这是保序换来的刻意权衡，暂不引入并行消费。
	for attempt := 0; ; attempt++ {
		err := c.applier.Apply(context.Background(), ev)
		if err == nil {
			slog.Info("已应用", "lsn", ev.LSN, "operation", ev.Operation, "table", ev.Schema+"."+ev.Table)
			msg.Ack(false)
			return
		}

		if attempt+1 >= mq.MaxAttempts {
			slog.Error("应用 CDC 事件失败，进死信", "lsn", ev.LSN, "operation", ev.Operation, "table", ev.Schema+"."+ev.Table, "err", err)
			msg.Nack(false, false)
			return
		}

		delay := mq.BackoffDelay(attempt)
		slog.Warn("应用失败，退避重试", "lsn", ev.LSN, "operation", ev.Operation, "table", ev.Schema+"."+ev.Table, "attempt", attempt+1, slog.Duration("delay", delay), "err", err)
		time.Sleep(delay)
	}
}
