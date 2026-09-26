package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/bobdfy/syncguard/internal/lock"
	"github.com/bobdfy/syncguard/internal/mq"
	"github.com/bobdfy/syncguard/internal/redisclient"
	"github.com/bobdfy/syncguard/internal/repository"
	"github.com/bobdfy/syncguard/internal/worker"
	"github.com/joho/godotenv"
)

func main() {
	// 加载 .env
	godotenv.Load()

	// 读取数据库地址
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL 未设置")
	}

	// 连接 PostgreSQL
	db, err := repository.NewDB(context.Background(), databaseURL)
	if err != nil {
		log.Fatalf("数据库连接失败: %v", err)
	}
	defer db.Close()

	//读取 RabbitMQ 连接地址
	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		log.Fatal("RABBITMQ_URL 未设置")
	}

	// 连接 RabbitMQ
	conn, ch, err := mq.Connect(rabbitURL)
	if err != nil {
		log.Fatalf("RabbitMQ 连接失败: %v", err)
	}
	defer conn.Close()
	defer ch.Close()

	// 读取 Redis 地址
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		log.Fatal("REDIS_ADDR 未设置")
	}
	redisPass := os.Getenv("REDIS_PASS")

	// 连接 Redis，创建连接级分布式锁
	rdb, err := redisclient.NewClient(context.Background(), redisAddr, redisPass)
	if err != nil {
		log.Fatalf("Redis 连接失败: %v", err)
	}
	defer rdb.Close()

	//创建一个连接级别的分布式锁，用于防止同一个连接被多个 Worker 同时使用
	connLock := lock.NewConnectionLock(rdb.Client())

	// 为延迟重投单独开一个发布 channel，并开启 publisher confirm
	publishCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("创建发布 channel 失败: %v", err)
	}
	defer publishCh.Close() //延迟关闭

	// 确保消息被 Broker 成功接收后才返回 ACK
	// false 表示不需要同步等待所有确认，性能较好
	if err := publishCh.Confirm(false); err != nil {
		log.Fatalf("开启 publisher confirm 失败: %v", err)
	}

	// 创建生产者实例，指定交换器、路由键（这些常量在 mq 包中定义）
	producer := mq.NewProducer(publishCh, mq.ExchangeName, mq.RoutingKey)

	// 创建 Consumer（内部设 Qos(prefetch=1)，一次只取一条）
	consumer, err := mq.NewConsumer(ch, mq.QueueName)
	if err != nil {
		log.Fatalf("创建 Consumer 失败: %v", err)
	}

	// 订阅队列，拿到消息 channel
	msgs, err := consumer.Consume()
	if err != nil {
		log.Fatalf("订阅队列失败: %v", err)
	}

	//组装outbox搬运工：扫描outbox未发送的信息 --> 发送到RabbitMQ
	//创建 outbox 数据存储层实例，用于操作 outbox 表
	outboxStore := repository.NewOutboxStore(db)

	//负责定期扫描 outbox 表，将 pending 消息通过 producer 发送到 RabbitMQ
	dispatcher := worker.NewDispatcher(producer, outboxStore)

	// 组装消息处理器
	// 创建 worker.Handler，负责处理从队列中消费到的每条消息（即执行任务）
	h := worker.New(db, connLock, rdb, outboxStore)

	log.Println("Worker 已启动，等待消息...")

	//启动搬运工：独立ctx， worker退出时一并取消
	dispatcherCtx, cancelDispatcher := context.WithCancel(context.Background())
	defer cancelDispatcher()

	// 启动搬运工，它会每隔一段时间扫描 outbox 表，发送未发送的消息
	go dispatcher.Start(dispatcherCtx)

	// 优雅退出：Ctrl+C 时退出循环
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)

	// 消息循环：阻塞等待消息或退出信号
	for {
		select {
		case <-quit:
			log.Println("正在关闭 Worker...")
			return

		case msg, ok := <-msgs:
			if !ok {
				log.Println("消息 channel 已关闭")
				return
			}
			h.Handle(msg)
		}
	}
}
