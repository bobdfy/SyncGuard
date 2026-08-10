package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Producer 表示 RabbitMQ 的消息生产者。
//
// 它的作用很简单：
//
//	业务代码
//	   ↓
//	Producer
//	   ↓
//	RabbitMQ Exchange
//	   ↓
//	Queue
//	   ↓
//	Worker
//
// 比如用户发起一个同步任务后，业务代码调用：
//
//	producer.Publish(ctx, 1001, "sync_user")
//
// Producer 会把任务信息转换成 JSON，
// 然后发送到 RabbitMQ。
//
// Producer 不关心 Worker 最后有没有执行成功，
// 它只负责把任务消息发送出去。
type Producer struct {
	// RabbitMQ Channel。
	//
	// 真正发送消息时，需要通过这个 Channel 调用 Publish。
	channel *amqp.Channel

	// 消息要发送到哪个 Exchange。
	//
	// 本项目通常是：
	//
	//     syncguard.jobs
	exchange string

	// RoutingKey，用于告诉 Exchange：
	// 这条消息应该按照什么规则进行路由。
	//
	// 本项目通常是：
	//
	//     job.run
	//
	// connection.go 中已经把：
	//
	//     Exchange + job.run
	//
	// 绑定到了主任务队列，
	// 所以消息最终会进入：
	//
	//     syncguard.jobs.queue
	routingKey string
}

// NewProducer 创建一个 Producer。
//
// 这里并不会创建新的 RabbitMQ 连接，
// 只是把已经创建好的 Channel、Exchange、RoutingKey
// 保存到 Producer 中，方便后续重复发送消息。
func NewProducer(ch *amqp.Channel, exchange string, routingKey string) *Producer {
	return &Producer{
		channel:    ch,
		exchange:   exchange,
		routingKey: routingKey,
	}
}

// Publish 发布一个任务消息到 RabbitMQ。
//
// 整个过程可以理解成 3 步：
//
//  1. 组装 JobMessage
//  2. 转成 JSON
//  3. 发布到 RabbitMQ Exchange
//
// 最终消息流向：
//
//	JobMessage
//	    ↓
//	  JSON
//	    ↓
//	Producer
//	    ↓
//	Exchange
//	    ↓
//	 Queue
//	    ↓
//	 Worker
func (p *Producer) Publish(ctx context.Context, jobID int, taskName string) error {

	// ========================================================
	// 1. 组装要发送的任务消息
	// ========================================================
	//
	// RabbitMQ 并不知道什么是 jobID、taskName。
	//
	// 这些都是我们自己定义的业务数据。
	//
	// 所以先把 Worker 之后执行任务需要的信息
	// 放进 JobMessage。
	msg := JobMessage{
		JobID:     jobID,
		TaskName:  taskName,
		Timestamp: time.Now(),
	}

	// ========================================================
	// 2. 把 Go 结构体转换成 JSON
	// ========================================================
	//
	// RabbitMQ 传输消息时，本质上传输的是一段二进制数据：
	//
	//     []byte
	//
	// 它不能直接传 Go 的 struct。
	//
	// 所以：
	//
	//     JobMessage
	//         ↓
	//     json.Marshal
	//         ↓
	//     []byte
	//
	// 假设 msg 是：
	//
	//     JobID:    123
	//     TaskName: "sync_user"
	//
	// 转成 JSON 后大致类似：
	//
	//     {
	//       "job_id": 123,
	//       "task_name": "sync_user",
	//       "timestamp": "..."
	//     }
	//
	// Worker 收到以后，再反过来使用 json.Unmarshal()
	// 把 JSON 还原成 JobMessage。
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("序列化任务消息失败: %w", err)
	}

	// ========================================================
	// 3. 把 JSON 包装成 RabbitMQ 消息
	// ========================================================
	//
	// body 只是普通的 []byte。
	//
	// RabbitMQ 发送消息时还可以附带一些额外信息，
	// 因此需要把它包装成 amqp.Publishing。
	pub := amqp.Publishing{
		// 告诉接收方：
		//
		//     “消息 Body 里面存的是 JSON。”
		//
		// RabbitMQ 本身不会解析这个 JSON，
		// 这个字段主要是描述消息的数据格式。
		ContentType: "application/json",

		// Persistent 表示这是一条持久化消息。
		//
		// 配合前面 durable=true 的 Queue，
		// 可以提高 RabbitMQ 重启时消息被保留下来的能力。
		//
		// 可以先简单理解成：
		//
		//     durable
		//         → Queue / Exchange 尽量保留下来
		//
		//     Persistent
		//         → 消息本身要求持久化
		//
		// 注意：
		// 这不等于“绝对保证消息永远不会丢失”。
		DeliveryMode: amqp.Persistent,

		// 真正的消息内容。
		//
		// 这里就是刚才 json.Marshal 得到的 JSON 字节数据。
		Body: body,
	}

	// ========================================================
	// 4. 把消息发布到 Exchange
	// ========================================================
	//
	// 这里非常重要：
	//
	// Producer 不是直接往 Queue 里面塞消息。
	//
	// 实际流程是：
	//
	//     Producer
	//         ↓
	//     Exchange
	//         ↓
	//     根据 RoutingKey 找 Queue
	//         ↓
	//       Queue
	//
	// 当前参数：
	//
	//     exchange   = syncguard.jobs
	//     routingKey = job.run
	//
	// connection.go 中已经存在这样的绑定：
	//
	//     syncguard.jobs
	//          │
	//          │ job.run
	//          ▼
	//     syncguard.jobs.queue
	//
	// 所以这条消息最终会进入主任务队列。
	err = p.channel.PublishWithContext(
		ctx,
		p.exchange,
		p.routingKey,

		// mandatory = false
		//
		// 如果 Exchange 找不到任何匹配的 Queue，
		// RabbitMQ 不要求把消息退回来给 Producer。
		//
		// 当前项目先使用 false 即可。
		false,

		// immediate = false
		//
		// RabbitMQ 已经不支持 immediate 功能，
		// 所以这里固定使用 false。
		false,

		pub,
	)
	if err != nil {
		return fmt.Errorf("发布任务消息失败: %w", err)
	}

	// 记录发送日志。
	//
	// 出问题时可以通过日志判断：
	//
	//     “业务代码到底有没有执行到发送消息这一步？”
	log.Printf(
		"[MQ] 已发布 job_id=%d task_name=%s",
		jobID,
		taskName,
	)

	return nil
}
