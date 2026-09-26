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

// outboxPublisher 投递能力的最小接口（*mq.Producer 天然满足），
// 便于单测注入 fake，不依赖真实 RabbitMQ。
type outboxPublisher interface {
	Publish(ctx context.Context, jobID int, taskName string, connectionID int, attempt int) error
	PublishDelayed(ctx context.Context, jobID int, taskName string, connectionID int, attempt int, delay time.Duration) error
}

// outboxStore 读取/标记 outbox 消息的最小接口（*repository.OutboxStore 天然满足）。
type outboxStore interface {
	ListUnsent(ctx context.Context, limit int) ([]repository.OutboxMessage, error)
	MarkSent(ctx context.Context, id int64) error
}

// Dispatcher 搬运工：定时扫描 outbox 未发送消息，发到 RabbitMQ。
type Dispatcher struct {
	producer outboxPublisher
	store    outboxStore
}

// NewDispatcher 组装搬运工。
func NewDispatcher(producer outboxPublisher, store outboxStore) *Dispatcher {
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
//  2. 循环每条：

// c. 发成功 → store.MarkSent；失败 → 记日志、继续（不标记，下轮自动重试）
func (d *Dispatcher) processOnce(ctx context.Context) error {
	// store.ListUnsent(ctx, 20) 拿最多 20 条未发送消息
	msgs, err := d.store.ListUnsent(ctx, 20)
	if err != nil {
		return fmt.Errorf("%w", err)
	}

	for _, msg := range msgs {
		var p mq.OutboxPayload
		// a. json.Unmarshal 解析出 mq.OutboxPayload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			log.Printf("[Dispatcher] 解析 outbox 消息 %d 失败: %v", msg.ID, err)
			continue //解析失败，等待人工处理
		}
		// b. DelayMs==0 → producer.Publish(...)；否则 producer.PublishDelayed(..., DelayMs 毫秒)
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
