package cdc

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/bobdfy/syncguard/internal/mq"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Serve 常驻消费 CDC 队列，把事件应用到目标库。阻塞直到 ctx 取消或消费 channel 关闭。
func Serve(ctx context.Context, ch *amqp.Channel, pool *pgxpool.Pool) error {
	sub, err := mq.NewConsumer(ch, mq.CDCQueueName)
	if err != nil {
		return fmt.Errorf("创建 CDC 消费者失败: %w", err)
	}
	msgs, err := sub.Consume()
	if err != nil {
		return fmt.Errorf("订阅 CDC 队列失败: %w", err)
	}

	handler := NewConsumer(NewApplier(pool))

	slog.Info("CDC 消费者已启动", "queue", mq.CDCQueueName)

	for {
		select {
		case <-ctx.Done():
			slog.Info("CDC 消费者已停止")
			return nil
		case msg, ok := <-msgs:
			if !ok {
				return fmt.Errorf("消息 channel 已关闭")
			}
			handler.Handle(msg)
		}
	}
}
