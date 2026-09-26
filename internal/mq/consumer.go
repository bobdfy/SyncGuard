package mq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Consumer 消息消费者：配置限流(QoS)、订阅队列、把消息交给 Worker。本身不处理业务。
type Consumer struct {
	channel *amqp.Channel // AMQP 操作（Consume/Qos/Ack/Nack）都在 Channel 上完成
	queue   string        // 要监听的队列名
}

// NewConsumer 创建 Consumer，并设置 QoS(prefetch=1)。
func NewConsumer(ch *amqp.Channel, queue string) (*Consumer, error) {
	if err := ch.Qos(1, 0, false); err != nil {
		return nil, fmt.Errorf("设置 Qos 失败: %w", err)
	}
	return &Consumer{
		channel: ch,
		queue:   queue,
	}, nil
}

// Consume 订阅队列，返回只读的 <-chan amqp.Delivery。
//
// RabbitMQ 每投递一条消息，amqp091-go 就包装成 amqp.Delivery 发到这个 channel。
// Worker 用 `for msg := range msgs` 消费；Channel 关闭时该 channel 也会关闭，循环自然结束。
func (c *Consumer) Consume() (<-chan amqp.Delivery, error) {
	msgs, err := c.channel.Consume(
		c.queue,
		"",    // consumerTag：自动生成
		false, // autoAck：关闭自动确认，由 Worker 手动 Ack/Nack
		false, // exclusive：允许多个 Consumer 共同消费
		false, // noLocal：RabbitMQ 不支持，固定 false
		false, // noWait：等待服务器确认订阅结果
		nil,   // args：无额外参数
	)
	if err != nil {
		return nil, fmt.Errorf("订阅队列失败: %w", err)
	}
	return msgs, nil
}
