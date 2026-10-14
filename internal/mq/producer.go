package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Producer 消息生产者，把任务序列化成 JSON 发到 RabbitMQ。
type Producer struct {
	channel    *amqp.Channel // 发送消息用的 Channel
	exchange   string        // 发到哪个 Exchange
	routingKey string        // 路由键
}

// NewProducer 复用已建好的 Channel，不新建连接。
func NewProducer(ch *amqp.Channel, exchange string, routingKey string) *Producer {
	return &Producer{
		channel:    ch,
		exchange:   exchange,
		routingKey: routingKey,
	}
}

// Publish 发布一条任务消息：组装 JobMessage → 序列化 JSON → 发到 Exchange。
func (p *Producer) Publish(ctx context.Context, jobID int, taskName string, connectionID int, attempt int) error {
	msg := JobMessage{
		JobID:        jobID,
		TaskName:     taskName,
		Timestamp:    time.Now(),
		ConnectionID: connectionID,
		Attempt:      attempt,
	}
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("序列化任务消息失败: %w", err)
	}

	// DeliveryMode=Persistent：消息本身要求持久化，配合 durable 队列提高重启后存活率
	pub := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	}

	// 两个 false 分别是 mandatory（找不到队列不退回）、immediate（已废弃，固定 false）
	err = p.channel.PublishWithContext(ctx, p.exchange, p.routingKey, false, false, pub)
	if err != nil {
		return fmt.Errorf("发布任务消息失败: %w", err)
	}

	// 结构化日志后 [MQ] 前缀就是冗余了：JSON 里每条日志都有字段可查，
	// 不再需要用文本前缀标识来源模块。
	slog.Info("已发布", "job_id", jobID, "task_name", taskName)
	return nil
}

// PublishDelayed 发布一条延迟消息到延迟重试队列。
func (p *Producer) PublishDelayed(ctx context.Context, jobID int, taskName string, connectionID int, attempt int, delay time.Duration) error {
	msg := JobMessage{
		JobID:        jobID,
		TaskName:     taskName,
		Timestamp:    time.Now(),
		ConnectionID: connectionID,
		Attempt:      attempt,
	}
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("序列化任务消息失败: %w", err)
	}

	pub := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Expiration:   strconv.FormatInt(delay.Milliseconds(), 10), // 消息级 TTL（毫秒）
		Body:         body,
	}

	// 发到延迟队列：默认 exchange("") + routing key=队列名，直发队列
	deferred, err := p.channel.PublishWithDeferredConfirmWithContext(
		ctx, "", RetryQueueName, false, false, pub,
	) //默认交换机 + 直发
	if err != nil {
		return fmt.Errorf("发布延迟消息失败: %w", err)
	}

	// 等 Broker 确认。err != nil = 超时没等到，!acked = 被 nack。
	acked, err := deferred.WaitContext(ctx)
	if err != nil || !acked {
		return fmt.Errorf("延迟消息未被 RabbitMQ 确认: %v", err)
	}

	// slog.Duration：Duration 是 int64 纳秒，直接当字段会输出成一串数字，
	slog.Info("已延迟发布", "job_id", jobID, "task", taskName, slog.Duration("delay", delay))
	return nil
}

// 把已经序列化好的字节原样发到当前 Producer 的 exchange/routingKey。
func (p *Producer) PublishRaw(ctx context.Context, body []byte) error {
	pub := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	}
	if err := p.channel.PublishWithContext(ctx, p.exchange, p.routingKey, false, false, pub); err != nil {
		return fmt.Errorf("发布原始消息失败: %w", err)
	}
	return nil
}

// 保证 Broker 能确认接收
// 返回 nil 才代表消息已到 Broker，调用方据此推进 processed_at
func (p *Producer) PublishRawConfirmed(ctx context.Context, body []byte) error {
	pub := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	}
	deferred, err := p.channel.PublishWithDeferredConfirmWithContext(ctx, p.exchange, p.routingKey, false, false, pub)
	if err != nil {
		return fmt.Errorf("发布原始消息失败: %w", err)
	}
	// 等 Broker 确认。err != nil = 超时/取消没等到，!acked = 被 nack。
	acked, err := deferred.WaitContext(ctx)
	if err != nil || !acked {
		return fmt.Errorf("原始消息未被 RabbitMQ 确认: %v", err)
	}
	return nil
}
