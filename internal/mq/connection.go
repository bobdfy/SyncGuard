package mq

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ============================================================
// RabbitMQ 拓扑配置
// ============================================================
//
// 在 RabbitMQ 中，Producer 一般不会直接把消息发送给 Queue，
// 而是先发送到 Exchange（交换机）。
//
// Exchange 再根据：
//     Exchange 类型 + RoutingKey + Binding
// 决定消息最终进入哪个 Queue。
//
// 本项目的正常消息链路：
//
// Producer
//    │
//    │ Publish(RoutingKey = "job.run")
//    ▼
// syncguard.jobs                  ← 主交换机 Exchange
//    │
//    │ BindingKey = "job.run"
//    ▼
// syncguard.jobs.queue            ← 主队列 Queue
//    │
//    ├── Worker处理成功
//    │       └── ACK
//    │           RabbitMQ 删除消息
//    │
//    └── Worker处理失败
//            └── NACK(requeue=false)
//                消息不重新进入原队列
//                ↓
// syncguard.jobs.dlx              ← 死信交换机 DLX
//    │
//    │ RoutingKey = "job.run"
//    ▼
// syncguard.jobs.dlq              ← 死信队列 DLQ
//
//
// 可以把它简单理解成：
//
// Exchange = 快递分拣中心
// Queue    = 快递仓库
// Binding  = 分拣规则
// RoutingKey = 快递上的地址标签
//
// Producer 只负责把消息送到“分拣中心”，
// RabbitMQ 根据路由规则把消息放进对应的“仓库”，
// Worker 再从仓库里取消息进行处理。

const (
	// 主交换机。
	// Producer 发布任务时，消息首先发送到这里。
	ExchangeName = "syncguard.jobs"

	// 主任务队列。
	// Worker 实际从这个队列读取并处理任务。
	QueueName = "syncguard.jobs.queue"

	// Dead Letter Exchange，死信交换机。
	//
	// 主队列中的消息如果因为某些原因成为“死信”，
	// RabbitMQ 会把消息重新发送到这个 Exchange。
	//
	// 本项目中最典型的情况：
	//
	//     Worker -> NACK(requeue=false)
	//
	// 表示：
	//     “这条消息处理失败，而且不要重新放回原队列。”
	//
	// RabbitMQ 随后会将它转发到 DLX。
	DLXName = "syncguard.jobs.dlx"

	// Dead Letter Queue，死信队列。
	//
	// 处理失败的消息最终暂存在这里，
	// 后续可以用于：
	//   1. 人工排查失败原因
	//   2. 查看原始任务数据
	//   3. 后续实现失败任务重试
	DLQName = "syncguard.jobs.dlq"

	// RoutingKey 可以理解成消息的“路由标签”。
	//
	// 因为这里 Exchange 使用 direct 类型，
	// 所以只有 RoutingKey 与 BindingKey 完全相等时，
	// 消息才会被投递到对应队列。
	//
	// Producer:
	//     Publish(..., "job.run", ...)
	//
	// Queue:
	//     QueueBind(..., "job.run", ...)
	//
	// 两边相同，因此消息可以成功进入队列。
	RoutingKey = "job.run"
)

// declareTopology 声明项目使用的 RabbitMQ 拓扑结构。
//
// “拓扑”可以理解成 RabbitMQ 中 Exchange、Queue、Binding
// 以及它们之间关系的整体配置。
//
// 这里会创建：
//
//  1. 死信交换机 DLX
//  2. 死信队列 DLQ
//  3. DLX -> DLQ 的绑定关系
//  4. 主交换机
//  5. 主任务队列
//  6. 主交换机 -> 主队列的绑定关系
//
// 为什么程序启动时要声明这些东西？
//
// 因为 Producer / Worker 在运行前，需要确保 RabbitMQ 中
// 对应的 Exchange 和 Queue 已经存在。
//
// RabbitMQ 的 Declare 操作可以重复执行：
// 如果同名 Exchange / Queue 已存在，并且配置完全相同，
// RabbitMQ 会直接返回成功，不会重复创建。
//
// 但需要注意：
// 如果名字相同、参数却不同，例如原来的 Queue 是 durable=true，
// 后来代码改成 durable=false，RabbitMQ 会报 PRECONDITION_FAILED。
// 所以修改已有队列的重要参数时，通常需要先删除旧队列再重新创建。
func declareTopology(ch *amqp.Channel) error {

	// ========================================================
	// 1. 声明死信交换机 DLX
	// ========================================================
	//
	// ExchangeDeclare 参数：
	//
	//   name       Exchange 名称
	//   kind       Exchange 类型
	//   durable    RabbitMQ 重启后 Exchange 是否仍然存在
	//   autoDelete 是否自动删除
	//   internal   是否只允许 RabbitMQ 内部转发
	//   noWait     是否不等待服务器响应
	//   args       额外参数
	//
	// 这里使用 direct Exchange：
	//
	//     RoutingKey == BindingKey
	//
	// 时才会完成投递。
	//
	// durable=true：
	//     RabbitMQ 服务重启之后，Exchange 定义仍然保留。
	//
	// 注意：
	// durable 只代表 Exchange / Queue 这种“结构”持久化，
	// 并不意味着其中的消息一定持久化。
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

	// ========================================================
	// 2. 声明死信队列 DLQ
	// ========================================================
	//
	// DLQ 本质上仍然只是一个普通 Queue。
	//
	// “死信队列”并不是 RabbitMQ 中一种特殊的 Queue 类型，
	// 它只是我们专门拿一个普通 Queue 来保存失败消息而已。
	//
	// durable=true：
	//     RabbitMQ 重启后队列结构仍然存在。
	//
	// exclusive=false：
	//     队列不属于某一个 Connection 独占，
	//     其他连接也可以访问。
	//
	// autoDelete=false：
	//     Consumer 断开后不会自动删除队列。
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

	// ========================================================
	// 3. 将死信队列绑定到死信交换机
	// ========================================================
	//
	// Queue 本身不会自动从 Exchange 获取消息，
	// 必须通过 Binding 建立二者之间的路由关系。
	//
	// 这里的关系是：
	//
	//     syncguard.jobs.dlx
	//              │
	//              │ RoutingKey = "job.run"
	//              ▼
	//     syncguard.jobs.dlq
	//
	// 当一条死信进入 DLX 时，
	// 如果它的 RoutingKey 是 "job.run"，
	// 就会匹配这里的 Binding，然后进入 DLQ。
	if err := ch.QueueBind(
		DLQName,
		RoutingKey,
		DLXName,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("绑定 DLQ: %w", err)
	}

	// ========================================================
	// 4. 声明主交换机
	// ========================================================
	//
	// Producer 发布任务时不会直接指定 Queue，
	// 而是把消息发送到这个 Exchange：
	//
	//     Producer
	//         ↓
	//     syncguard.jobs
	//
	// 然后由 Exchange 根据 RoutingKey 决定进入哪个队列。
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

	// ========================================================
	// 5. 声明主任务队列，并配置死信交换机
	// ========================================================
	//
	// 这里最重要的是：
	//
	//     x-dead-letter-exchange
	//
	// 它告诉 RabbitMQ：
	//
	//     “如果这个 Queue 中的消息变成死信，
	//      请把消息发送到哪个 Exchange？”
	//
	// 本项目指定：
	//
	//     x-dead-letter-exchange = syncguard.jobs.dlx
	//
	// 因此：
	//
	// syncguard.jobs.queue
	//          │
	//          │ 消息成为死信
	//          ▼
	// syncguard.jobs.dlx
	//
	// RabbitMQ 中常见的死信情况包括：
	//
	//   1. Consumer 执行 NACK(requeue=false)
	//   2. Consumer 执行 Reject(requeue=false)
	//   3. 消息 TTL 过期
	//   4. 队列达到长度限制
	//
	// 我们这里主要使用的是第 1 种。
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

	// ========================================================
	// 6. 将主任务队列绑定到主交换机
	// ========================================================
	//
	// 到这里才真正建立：
	//
	//     Exchange
	//         ↓
	//       Queue
	//
	// 的路由关系。
	//
	// 当前使用 direct Exchange，因此 Producer 发布：
	//
	//     RoutingKey = "job.run"
	//
	// 时，会和这里的 BindingKey：
	//
	//     "job.run"
	//
	// 完全匹配，消息于是进入 QueueName。
	if err := ch.QueueBind(
		QueueName,
		RoutingKey,
		ExchangeName,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("绑定主队列: %w", err)
	}

	log.Println("[MQ] RabbitMQ 拓扑声明完成")
	return nil
}

// Connect 创建 RabbitMQ Connection 和 Channel，
// 并确保项目所需要的 Exchange / Queue / Binding 已经存在。
//
// RabbitMQ 中需要区分两个概念：
//
//	Connection
//	    真正的 TCP 网络连接。
//	    创建成本相对较高，一般整个服务复用少量 Connection。
//
//	Channel
//	    建立在 Connection 上的逻辑通信通道。
//	    Publish、Consume、QueueDeclare 等操作基本都通过 Channel 完成。
//
// 可以简单理解为：
//
//	Connection = 一条真正的高速公路
//	Channel    = 高速公路上的不同车道
//
// 不需要每发布一条消息就重新创建 Connection。
//
// 返回：
//
//	conn  RabbitMQ TCP 连接
//	ch    基于该连接创建的 Channel
//	err   初始化过程中发生的错误
//
// 调用方在程序退出时需要关闭：
//
//	ch.Close()
//	conn.Close()
func Connect(url string) (*amqp.Connection, *amqp.Channel, error) {

	// --------------------------------------------------------
	// 1. 建立到 RabbitMQ Broker 的 TCP Connection
	// --------------------------------------------------------
	//
	// URL 一般类似：
	//
	//     amqp://guest:guest@localhost:5672/
	//
	// Dial 成功只能说明已经连接到了 RabbitMQ，
	// 此时 Exchange 和 Queue 不一定已经存在。
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, fmt.Errorf("RabbitMQ 连接失败: %w", err)
	}

	// --------------------------------------------------------
	// 2. 在 Connection 上创建 Channel
	// --------------------------------------------------------
	//
	// RabbitMQ 的绝大多数操作都是通过 Channel 完成的。
	//
	// 如果创建 Channel 失败，
	// 前面已经创建成功的 Connection 必须关闭，
	// 避免连接资源泄漏。
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("创建 Channel 失败: %w", err)
	}

	// --------------------------------------------------------
	// 3. 初始化 RabbitMQ 拓扑
	// --------------------------------------------------------
	//
	// 确保下面这些资源已经存在：
	//
	//     主 Exchange
	//     主 Queue
	//     死信 Exchange
	//     死信 Queue
	//     对应 Binding
	//
	// 如果初始化失败，
	// 当前 Channel 和 Connection 都已经没有继续保留的意义，
	// 因此统一关闭。
	if err := declareTopology(ch); err != nil {
		ch.Close()
		conn.Close()
		return nil, nil, fmt.Errorf("声明拓扑失败: %w", err)
	}

	log.Println("[MQ] RabbitMQ 已连接")
	return conn, ch, nil
}
