package mq

import (
	"fmt"
	"log/slog"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// 消息链路：
//
//	Producer → Exchange(syncguard.jobs) → Queue(syncguard.jobs.queue) → Worker
const (
	ExchangeName = "syncguard.jobs"       // 主交换机
	QueueName    = "syncguard.jobs.queue" // 主队列
	DLXName      = "syncguard.jobs.dlx"   // 死信交换机
	DLQName      = "syncguard.jobs.dlq"   // 死信队列

	// RoutingKey：direct 交换机下 RoutingKey==BindingKey 才投递。
	RoutingKey = "job.run"

	// CDC 专用通道
	CDCExchangeName = "syncguard.cdc"
	CDCQueueName    = "syncguard.cdc.queue"
	CDCRoutingKey   = "cdc.change"

	// CDC 死信：应用失败的 CDC 事件落到这里，不丢
	CDCDLXExchangeName = "syncguard.cdc.dlx"
	CDCDLQQueueName    = "syncguard.cdc.dlq"
	CDCDLQRoutingKey   = "cdc.dlq"
)

// retryTiers 延迟重试档位（从小到大）。每个档位一个固定 x-message-ttl 的队列，
// 用队列级 TTL 替代消息级 Expiration：RabbitMQ 只在队头检查消息级 TTL，单队列会队头阻塞。
// 最大档位覆盖 backoffMax(2min)+抖动(~3min)，避免超档被钳到 60s 提前重试。
var retryTiers = []time.Duration{
	5 * time.Second,
	10 * time.Second,
	20 * time.Second,
	30 * time.Second,
	40 * time.Second,
	60 * time.Second,
	120 * time.Second,
	180 * time.Second,
}

// retryQueueName 把延迟向上取整到最近的档位，返回对应队列名；超出最大档位用最大档位。
func retryQueueName(delay time.Duration) string {
	tier := retryTiers[len(retryTiers)-1]
	for _, t := range retryTiers {
		if delay <= t {
			tier = t
			break
		}
	}
	return "syncguard.jobs.retry." + strconv.FormatInt(int64(tier/time.Second), 10) + "s"
}

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

	// 7. 延迟重试队列：按档位拆多个固定 x-message-ttl 的队列。DLX 指向主交换机（不是 DLX），
	// 队列 TTL 到期后消息死信回主队列重试。拆档后同队列 TTL 相同、按入队顺序过期，互不阻塞。
	for _, tier := range retryTiers {
		retryArgs := amqp.Table{
			"x-dead-letter-exchange":    ExchangeName,
			"x-dead-letter-routing-key": RoutingKey,
			"x-message-ttl":             int64(tier / time.Millisecond),
		}
		if _, err := ch.QueueDeclare(
			retryQueueName(tier),
			true,
			false,
			false,
			false,
			retryArgs,
		); err != nil {
			return fmt.Errorf("声明延迟队列: %w", err)
		}
	}

	// CDC 交换机
	if err := ch.ExchangeDeclare(CDCExchangeName, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("声明 CDC 交换机失败: %w", err)
	}

	// CDC 死信交换机 + 死信队列（终态，应用失败的消息最终落在这里）
	if err := ch.ExchangeDeclare(CDCDLXExchangeName, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("声明 CDC 死信交换机失败: %w", err)
	}
	if _, err := ch.QueueDeclare(CDCDLQQueueName, true, false, false, false, nil); err != nil {
		return fmt.Errorf("声明 CDC 死信队列失败: %w", err)
	}
	if err := ch.QueueBind(CDCDLQQueueName, CDCDLQRoutingKey, CDCDLXExchangeName, false, nil); err != nil {
		return fmt.Errorf("绑定 CDC 死信队列: %w", err)
	}

	// CDC 队列，配置死信：Nack(requeue=false) 的消息进 CDC 死信交换机
	cdcQueueArgs := amqp.Table{
		"x-dead-letter-exchange":    CDCDLXExchangeName,
		"x-dead-letter-routing-key": CDCDLQRoutingKey,
	}
	if _, err := ch.QueueDeclare(CDCQueueName, true, false, false, false, cdcQueueArgs); err != nil {
		return fmt.Errorf("声明 CDC 队列失败: %w", err)
	}

	// CDC 交换机 -> 队列绑定
	if err := ch.QueueBind(CDCQueueName, CDCRoutingKey, CDCExchangeName, false, nil); err != nil {
		return fmt.Errorf("绑定 CDC 队列: %w", err)
	}

	slog.Info("RabbitMQ 拓扑声明完成")
	return nil

}

// Connect 建立 Connection 和 Channel，并声明拓扑。
//
// Connection = 一条 TCP 连接（复用少量）；Channel = 其上的逻辑通道（实际收发消息都走它）。任一步失败会关闭已创建的资源，避免泄漏。
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

	slog.Info("RabbitMQ 已连接")
	return conn, ch, nil
}
