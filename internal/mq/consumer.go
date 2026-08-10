package mq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ============================================================
// Consumer：RabbitMQ 消息消费者
// ============================================================
//
// Consumer 的职责比较单一：
//
//  1. 配置当前 Channel 的消费限流策略（QoS）
//  2. 订阅指定 Queue
//  3. 把 RabbitMQ 推送过来的消息交给 Worker
//
// Consumer 本身不处理业务逻辑。
//
// 也就是说，这里不会：
//
//   - 解析任务 JSON
//   - 执行业务任务
//   - 判断任务成功还是失败
//   - ACK / NACK 消息
//
// 这些事情应该由真正负责执行任务的 Worker 完成。
//
// 整体关系可以理解为：
//
// RabbitMQ Queue
//
//	    │
//	    │ 推送 Delivery
//	    ▼
//	Consumer
//	    │
//	    │ Go channel
//	    ▼
//	  Worker
//	    │
//	    ├── 处理成功 → ACK
//	    │
//	    └── 处理失败 → NACK
//
// Consumer 更像是 RabbitMQ 与 Worker 之间的“消息接收层”。
type Consumer struct {
	// RabbitMQ Channel。
	//
	// QueueConsume、Qos、Ack/Nack 等 AMQP 操作，
	// 都是在 Channel 上完成的。
	//
	// 这个 Channel 通常由 connection.go 中的 Connect() 创建。
	channel *amqp.Channel

	// 当前 Consumer 要监听的队列名。
	//
	// 本项目通常传入：
	//
	//     QueueName = "syncguard.jobs.queue"
	//
	// Producer 发出的任务经过 Exchange 路由以后进入这个 Queue，
	// Consumer 再从这里接收任务。
	queue string
}

// NewConsumer 创建一个 RabbitMQ Consumer。
//
// 创建 Consumer 时会先设置 QoS：
//
//	prefetchCount = 1
//
// 它的核心作用是限制“尚未 ACK/NACK 的消息数量”。
//
// ------------------------------------------------------------
// 为什么需要 prefetch？
// ------------------------------------------------------------
//
// RabbitMQ 消费模型不是 Worker 每处理完一条消息以后，
// 再主动向服务器发送一次“给我下一条”的请求。
//
// 更接近真实情况的是：
//
//	RabbitMQ
//	   │
//	   │ 主动推送
//	   ▼
//	Consumer
//
// RabbitMQ 可以提前把消息推送给 Consumer。
//
// 假设队列里有：
//
//	Job1
//	Job2
//	Job3
//	Job4
//	Job5
//
// 如果不合理限制 prefetch，某个 Consumer 可能提前拿到很多
// 尚未完成处理的消息：
//
//	Consumer A：Job1 Job2 Job3 Job4 Job5 ...
//
// 这些消息虽然还没有 ACK，但已经被分配给 Consumer A，
// 其他 Consumer 就不能立即处理它们。
//
// 如果任务执行时间比较长，就可能出现：
//
//	Worker A：手里积压很多任务，非常忙
//	Worker B：手里没有任务，相对空闲
//
// 这种负载就不够均衡。
//
// 设置：
//
//	prefetchCount = 1
//
// 后效果大致变成：
//
// RabbitMQ
//
//	│
//	│ Job1
//	▼
//
// Worker
//
//	│
//	│ 正在处理 Job1
//	│
//	│ 此时 Job1 尚未 ACK
//	│
//	└── RabbitMQ 暂时不会继续给该消费者发送 Job2
//
// Worker
//
//	│
//	└── ACK Job1
//	       ↓
//
// RabbitMQ
//
//	│
//	└── 可以继续发送 Job2
//
// 因此 prefetch=1 特别适合：
//
//   - 单个任务执行时间较长
//   - 每个 Worker 同时只希望处理一个任务
//   - 希望多个 Worker 之间尽量公平分配任务
//
// 代价是吞吐量可能不如较大的 prefetch。
// 如果以后 Worker 支持并发执行多个任务，
// prefetchCount 也可以相应调大。
func NewConsumer(ch *amqp.Channel, queue string) (*Consumer, error) {

	// --------------------------------------------------------
	// 配置 Consumer QoS
	// --------------------------------------------------------
	//
	// Qos(prefetchCount, prefetchSize, global)
	//
	// prefetchCount = 1
	//
	//     当前消费者最多拥有 1 条“已经收到但尚未确认”的消息。
	//
	//     注意这里限制的不是：
	//
	//         “一次调用 Consume() 只能取一条”
	//
	//     而是：
	//
	//         “最多允许 1 条 unacked message”
	//
	//     当这一条消息被 Ack/Nack 后，
	//     RabbitMQ 才有空间继续向消费者发送下一条。
	//
	// prefetchSize = 0
	//
	//     不按照消息字节大小设置 prefetch 限制。
	//
	// global = false
	//
	//     不把这个 prefetch 设置作为整个 Channel 的全局共享限制。
	//     对我们当前“一条 Channel 对应一个 Consumer”的使用方式来说，
	//     可以简单理解为限制当前 Consumer 的未确认消息数量。
	if err := ch.Qos(1, 0, false); err != nil {
		return nil, fmt.Errorf("设置 Qos 失败: %w", err)
	}

	//这里只保存 Consumer 后续工作需要的两个信息：
	//
	//     1. 使用哪个 RabbitMQ Channel
	//     2. 监听哪个 Queue
	//
	// 真正开始接收消息是在 Consume() 中进行。
	return &Consumer{
		channel: ch,
		queue:   queue,
	}, nil
}

// Consume  Consume 开始监听 RabbitMQ 队列，并接收队列中的消息。
//
// 调用成功后，会返回：
//
//	<-chan amqp.Delivery
//
// 这是一个 Go 的只读 Channel。
//
// RabbitMQ 每向当前消费者投递一条消息，
// amqp091-go 就会把它包装成一个 amqp.Delivery，
// 然后发送到这个 Go channel 中。
//
// 所以 Worker 通常会这样消费：
//
//	msgs, err := consumer.Consume()
//	if err != nil {
//	    ...
//	}
//
//	for msg := range msgs {
//	    // 1. 解析 msg.Body
//	    // 2. 执行业务
//	    // 3. 成功 -> msg.Ack(false)
//	    // 4. 失败 -> msg.Nack(false, false)
//	}
//
// ------------------------------------------------------------
// RabbitMQ Queue 和 Go channel 不要混淆
// ------------------------------------------------------------
//
// 这里实际上存在两种完全不同的“队列/通道”：
//
// ① RabbitMQ Queue
//
//	syncguard.jobs.queue
//
//	存在于 RabbitMQ Server 中，
//	用来持久保存等待消费的任务。
//
// ② Go channel
//
//	<-chan amqp.Delivery
//
//	存在于当前 Go 进程内存中，
//	是 amqp091-go 把 RabbitMQ 消息交给 Worker 的方式。
//
// 消息链路是：
//
// RabbitMQ Queue
//
//	│
//	│ AMQP 网络协议
//	▼
//
// amqp091-go
//
//	│
//	│ amqp.Delivery
//	▼
//
// Go channel
//
//	   │
//	   ▼
//	Worker
//
// 所以 Consume() 返回的不是 RabbitMQ Queue 本身，
// 而是本地 Go 程序中用于接收消息的 channel。
func (c *Consumer) Consume() (<-chan amqp.Delivery, error) {

	// --------------------------------------------------------
	// 开始订阅 Queue
	// --------------------------------------------------------
	//
	// Consume(
	//     queue,
	//     consumerTag,
	//     autoAck,
	//     exclusive,
	//     noLocal,
	//     noWait,
	//     args,
	// )
	//
	// -------------------------
	// queue = c.queue
	// -------------------------
	//
	// 指定当前 Consumer 要监听哪个 RabbitMQ Queue。
	//
	// 本项目通常是：
	//
	//     syncguard.jobs.queue
	//
	//
	// -------------------------
	// consumerTag = ""
	// -------------------------
	//
	// Consumer Tag 是 RabbitMQ 用于识别某个消费者的标识。
	//
	// 这里传空字符串，让 RabbitMQ / 客户端自动生成即可。
	//
	// 如果以后需要：
	//
	//     - 后台管理中识别具体 Worker
	//     - 主动 Cancel 某个 Consumer
	//     - 更容易观察 RabbitMQ 管理界面
	//
	// 也可以为不同 Worker 设置明确的 consumerTag。
	//
	//
	// -------------------------
	// autoAck = false
	// -------------------------
	//
	// 这是这里最重要的参数之一。
	//
	// false 表示关闭自动确认：
	//
	//     RabbitMQ 把消息交给 Worker
	//              ↓
	//       消息不会立即删除
	//              ↓
	//       等待 Worker 明确 ACK/NACK
	//
	// Worker 成功：
	//
	//     msg.Ack(false)
	//
	// RabbitMQ 才认为：
	//
	//     “这个任务已经安全处理完成。”
	//
	// 然后删除该消息。
	//
	// Worker 失败：
	//
	//     msg.Nack(false, false)
	//
	// 第二个 false 表示：
	//
	//     requeue = false
	//
	// 即不要重新塞回原队列。
	//
	// 由于主队列配置了：
	//
	//     x-dead-letter-exchange
	//
	// 消息会因此成为死信，并进入 DLX / DLQ。
	//
	//
	// 如果这里设置：
	//
	//     autoAck = true
	//
	// 那么 RabbitMQ 在把消息交给客户端后，
	// 就会直接把它视为已经成功消费。
	//
	// 此时即使发生：
	//
	//     收到消息
	//        ↓
	//     Worker 开始执行
	//        ↓
	//     程序突然崩溃
	//
	// RabbitMQ 也无法重新处理这条任务，
	// 因为它之前已经被自动确认了。
	//
	// 对“任务执行系统”来说通常不合适，
	// 所以这里必须使用手动 ACK。
	//
	//
	// -------------------------
	// exclusive = false
	// -------------------------
	//
	// 表示这个 Queue 不只允许当前 Consumer 消费。
	//
	// 因此未来完全可以启动多个 Worker：
	//
	//                  ┌── Worker 1
	// RabbitMQ Queue ──┼── Worker 2
	//                  └── Worker 3
	//
	// RabbitMQ 会把不同消息分发给不同 Consumer。
	//
	// 这也是 RabbitMQ 很常见的“竞争消费者”模式：
	//
	//     Competing Consumers
	//
	// 多个 Worker 共同消费同一个任务队列，
	// 从而实现横向扩容。
	//
	//
	// -------------------------
	// noLocal = false
	// -------------------------
	//
	// AMQP 协议历史上定义了 no-local 语义：
	//
	//     不接收由当前连接自己发布的消息。
	//
	// 但 RabbitMQ 并没有实现这个特性，
	// 因此这里固定使用 false。
	//
	//
	// -------------------------
	// noWait = false
	// -------------------------
	//
	// false 表示等待 RabbitMQ 确认订阅操作。
	//
	// 如果订阅失败，例如：
	//
	//     - Queue 不存在
	//     - Channel 已关闭
	//     - 参数不合法
	//
	// Consume() 会返回 error。
	//
	//
	// -------------------------
	// args = nil
	// -------------------------
	//
	// 当前没有额外的 Consumer 参数。
	msgs, err := c.channel.Consume(
		c.queue, // RabbitMQ Queue 名称
		"",      // consumerTag：自动生成
		false,   // autoAck：关闭自动确认，由 Worker 手动 ACK/NACK
		false,   // exclusive：允许多个 Consumer 共同消费
		false,   // noLocal：RabbitMQ 不支持 no-local，固定 false
		false,   // noWait：等待服务器确认订阅结果
		nil,     // args：无额外参数
	)
	if err != nil {
		return nil, fmt.Errorf("订阅队列失败: %w", err)
	}

	// --------------------------------------------------------
	// 返回消息流给 Worker
	// --------------------------------------------------------
	//
	// msgs 是一个只读 Go channel：
	//
	//     <-chan amqp.Delivery
	//
	// “只读”意味着调用方可以：
	//
	//     msg := <-msgs
	//
	// 但不能：
	//
	//     msgs <- xxx
	//
	// 这样设计很合理，因为这个 channel 的生产者应该只有
	// RabbitMQ 客户端库，Worker 只负责接收并处理消息。
	//
	// 当 RabbitMQ Connection / Channel 被关闭，
	// 或 Consumer 被取消时，这个 Go channel 最终也会关闭，
	// Worker 的：
	//
	//     for msg := range msgs
	//
	// 会自然结束。
	return msgs, nil
}
