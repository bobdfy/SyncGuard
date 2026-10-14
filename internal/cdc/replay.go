package cdc

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/bobdfy/syncguard/internal/mq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Replay 把 CDC 死信队列里的消息全部补投回 CDC 主队列，返回补投条数
func Replay(ch *amqp.Channel) (int, error) {
	producer := mq.NewProducer(ch, mq.CDCExchangeName, mq.CDCRoutingKey)

	replayed := 0
	for {
		msg, ok, err := ch.Get(mq.CDCDLQQueueName, false) // autoAck=false：补投成功才确认
		if err != nil {
			return replayed, fmt.Errorf("读取死信队列失败: %w", err)
		}
		if !ok {
			break // 队列已空
		}

		if err := producer.PublishRaw(context.Background(), msg.Body); err != nil {
			slog.Error("补投失败，消息留在死信队列", "err", err)
			_ = msg.Nack(false, true) // requeue=true 放回死信队列，别丢
			continue
		}
		msg.Ack(false)
		replayed++
		slog.Info("已补投一条", "total", replayed)
	}
	return replayed, nil
}
