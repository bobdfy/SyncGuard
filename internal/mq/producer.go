package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Producer 消息生产者，把任务序列化成 JSON 发到 RabbitMQ。
// 只负责「发出去」，不关心 Worker 最后是否执行成功。
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

	log.Printf("[MQ] 已发布 job_id=%d task_name=%s", jobID, taskName)
	return nil
}

// PublishDelayed 发布一条延迟消息到延迟重试队列。
//
// 用途：Worker 抢锁失败（新触发）时，把任务延迟 delay 时间后重新投递，避免忙等。
//
// 原理：消息带 Expiration（消息级 TTL，毫秒）进入延迟队列，到期成为死信，
// 被队列的 x-dead-letter-exchange 转发回主交换机 → 重新进入主队列。
//
// 可靠性：Publisher Confirm —— 必须等到 Broker 返回 basic.ack 才返回 nil；
// nack 或超时都视为失败。调用方拿到 nil 才能确定「延迟消息已被接收」，
// 进而安全 Ack 原消息，避免丢消息。
//
// 前提：此方法所在的 Channel 必须先 Confirm(false) 开启 publisher confirm，
// 否则 PublishWithDeferredConfirm 会报错。
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

	log.Printf("[MQ] 已延迟发布 job_id=%d（%s 后重试）", jobID, delay)
	return nil
}
