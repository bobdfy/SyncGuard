package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/bobdfy/syncguard/internal/mq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// ReplayJobs 把 jobs 死信队列里的消息全部补投回主队列，返回补投条数。
//
// 与 CDC 的 Replay 不同：jobs 死信里的消息 Attempt 已到上限，原样重投会被
// worker 立刻再次判死（settle 里 attempt>=MaxAttempts 直接 Nack 进 DLQ）。
// 所以这里解析后把 Attempt 清零再重投，等于给任务一次全新的重试机会。
func ReplayJobs(ch *amqp.Channel) (int, error) {
	producer := mq.NewProducer(ch, mq.ExchangeName, mq.RoutingKey)

	replayed := 0
	for {
		msg, ok, err := ch.Get(mq.DLQName, false) // autoAck=false：补投成功才确认
		if err != nil {
			return replayed, fmt.Errorf("读取死信队列失败: %w", err)
		}
		if !ok {
			break // 队列已空
		}

		var jm mq.JobMessage
		if err := json.Unmarshal(msg.Body, &jm); err != nil {
			// 解析不了 = 永久损坏，重投只会再进一次 DLQ，直接丢弃并告警。
			slog.Error("死信消息解析失败，丢弃", "err", err)
			msg.Nack(false, false)
			continue
		}

		// Attempt 传 0：Producer 会重建一条全新消息（Timestamp 刷新、Attempt 清零）。
		if err := producer.Publish(context.Background(), jm.JobID, jm.TaskName, jm.ConnectionID, 0); err != nil {
			// 补投失败：消息 requeue 放回死信队列（不丢），直接返回而非 continue——
			// 否则下一轮 Get 又取到同一条，Publish 持续失败时无限自旋（与 cdc/replay 同坑）。
			_ = msg.Nack(false, true)
			return replayed, fmt.Errorf("补投失败: %w", err)
		}
		msg.Ack(false)
		replayed++
		slog.Info("已补投一条", "total", replayed, "job_id", jm.JobID)
	}
	return replayed, nil
}
