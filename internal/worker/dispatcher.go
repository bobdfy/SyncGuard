package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/bobdfy/syncguard/internal/mq"
	"github.com/bobdfy/syncguard/internal/repository"
)

// Dispatcher 搬运工：定时扫描 outbox 未发送消息，发到 RabbitMQ。
type Dispatcher struct {
	producer *mq.Producer
	store    *repository.OutboxStore
}

// NewDispatcher 组装搬运工。
func NewDispatcher(producer *mq.Producer, store *repository.OutboxStore) *Dispatcher {
	return &Dispatcher{producer: producer, store: store}
}

// Start 启动搬运循环：每 2 秒扫一次未发送消息并发送；ctx 取消时退出。
func (d *Dispatcher) Start(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Println("[Dispatcher] 已停止")
			return
		case <-ticker.C:
			if err := d.processOnce(ctx); err != nil {
				log.Printf("[Dispatcher] 扫描发送失败: %v", err)
			}
		}
	}
}

// processOnce 扫一轮：查未发送 → 逐条发送 → 成功标 sent_at；失败留到下轮重试。
//
// 步骤：
//  1. store.ListUnsent(ctx, 20) 拿最多 20 条未发送消息
//  2. 循环每条：
//     a. json.Unmarshal 解析出 mq.OutboxPayload
//     b. DelayMs==0 → producer.Publish(...)；否则 producer.PublishDelayed(..., DelayMs 毫秒)
//     c. 发成功 → store.MarkSent；失败 → 记日志、继续（不标记，下轮自动重试）
func (d *Dispatcher) processOnce(ctx context.Context) error {
	msgs, err := d.store.ListUnsent(ctx, 20)
	if err != nil {
		return fmt.Errorf("%w", err)
	}

	for _, msg := range msgs {
		var p mq.OutboxPayload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			log.Printf("[Dispatcher] 解析 outbox 消息 %d 失败: %v", msg.ID, err)
			continue //解析失败，等待人工处理
		}

		var sendErr error
		if p.DelayMs == 0 {
			//立即发出主队列
			sendErr = d.producer.Publish(ctx, p.JobID, p.TaskName, p.ConnectionID, p.Attempt)
		} else {
			//延迟发
			sendErr = d.producer.PublishDelayed(ctx, p.JobID, p.TaskName, p.ConnectionID, p.Attempt, time.Duration(p.DelayMs)*time.Millisecond)
		}

		if sendErr != nil {
			//发送失败: 不标sent，下一轮重试
			log.Printf("[Dispatcher] 发送 outbox 消息 %d 失败: %v", msg.ID, sendErr)
			continue
		}
		if err := d.store.MarkSent(ctx, msg.ID); err != nil {
			log.Printf("[Dispatcher] 标记 outbox 消息 %d 失败: %v", msg.ID, err)
			// 标记失败也继续：消息可能已发出但没标上，下轮会重发（at-least-once，靠幂等兜底）
			continue

		}
	}
	return nil
}
