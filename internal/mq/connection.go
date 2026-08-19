package mq

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

// 消息链路：
//
//	Producer → Exchange(syncguard.jobs) → Queue(syncguard.jobs.queue) → Worker
//
//	处理失败：Nack(requeue=false) → DLX → DLQ（死信）
//	抢锁失败：延迟队列(retry) 的 DLX 指向主交换机，消息 TTL 过期后回主队列重试
const (
	ExchangeName = "syncguard.jobs"       // 主交换机
	QueueName    = "syncguard.jobs.queue" // 主队列
	DLXName      = "syncguard.jobs.dlx"   // 死信交换机
	DLQName      = "syncguard.jobs.dlq"   // 死信队列

	// RetryQueueName 延迟重试队列。抢锁失败（新触发）的消息带 Expiration(TTL)
	// 发到这里，到期成为死信，被 x-dead-letter-exchange 转发回主交换机 → 重新进入主队列。
	// 注意：这里的 DLX 指向主交换机，不是死信 DLX。
	RetryQueueName = "syncguard.jobs.retry"

	// RoutingKey：direct 交换机下 RoutingKey==BindingKey 才投递。
	RoutingKey = "job.run"
)

// declareTopology 声明 Exchange/Queue/Binding，确保运行前拓扑存在。
//
// Declare 幂等：同名且配置一致时直接成功；改参数需先删旧队列，否则 PRECONDITION_FAILED。
func declareTopology(ch *amqp.Channel) error {
	// 1. 死信交换机
	if err := ch.ExchangeDeclare(
		DLXName,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("声明 DLX: %w", err)
	}

	// 2. 死信队列
	if _, err := ch.QueueDeclare(
		DLQName,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("声明 DLQ: %w", err)
	}

	// 3. DLX -> DLQ 绑定
	if err := ch.QueueBind(
		DLQName,
		RoutingKey,
		DLXName,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("绑定 DLQ: %w", err)
	}

	// 4. 主交换机
	if err := ch.ExchangeDeclare(
		ExchangeName,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("声明主交换机: %w", err)
	}

	// 5. 主队列，配置死信：Nack(requeue=false) 的消息进 DLX
	queueArgs := amqp.Table{
		"x-dead-letter-exchange": DLXName,
	}

	if _, err := ch.QueueDeclare(
		QueueName,
		true,
		false,
		false,
		false,
		queueArgs,
	); err != nil {
		return fmt.Errorf("声明主队列: %w", err)
	}

	// 6. 主交换机 -> 主队列绑定
	if err := ch.QueueBind(
		QueueName,
		RoutingKey,
		ExchangeName,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("绑定主队列: %w", err)
	}

	// 7. 延迟重试队列。DLX 指向主交换机（不是 DLX），消息 TTL 过期后回主队列重试。
	retryArgs := amqp.Table{
		"x-dead-letter-exchange":    ExchangeName,
		"x-dead-letter-routing-key": RoutingKey,
	}

	if _, err := ch.QueueDeclare(
		RetryQueueName,
		true,
		false,
		false,
		false,
		retryArgs,
	); err != nil {
		return fmt.Errorf("声明延迟队列: %w", err)
	}

	log.Println("[MQ] RabbitMQ 拓扑声明完成")
	return nil
}

// Connect 建立 Connection 和 Channel，并声明拓扑。
//
// Connection = 一条 TCP 连接（复用少量）；Channel = 其上的逻辑通道（实际收发消息都走它）。
// 调用方退出时需 ch.Close()、conn.Close()；任一步失败会关闭已创建的资源，避免泄漏。
func Connect(url string) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, fmt.Errorf("RabbitMQ 连接失败: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("创建 Channel 失败: %w", err)
	}

	if err := declareTopology(ch); err != nil {
		ch.Close()
		conn.Close()
		return nil, nil, fmt.Errorf("声明拓扑失败: %w", err)
	}

	log.Println("[MQ] RabbitMQ 已连接")
	return conn, ch, nil
}
