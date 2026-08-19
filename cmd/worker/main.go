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

	// 读取环境变量
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL 未设置")
	}

	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		log.Fatal("RABBITMQ_URL 未设置")
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		log.Fatal("REDIS_ADDR 未设置")
	}
	redisPass := os.Getenv("REDIS_PASS")

	// 连接 PostgreSQL
	db, err := repository.NewDB(context.Background(), databaseURL)
	if err != nil {
		log.Fatalf("数据库连接失败: %v", err)
	}
	defer db.Close()

	// 连接 RabbitMQ
	conn, ch, err := mq.Connect(rabbitURL)
	if err != nil {
		log.Fatalf("RabbitMQ 连接失败: %v", err)
	}
	defer conn.Close()
	defer ch.Close()

	// 连接 Redis，创建连接级分布式锁
	rdb, err := redisclient.NewClient(context.Background(), redisAddr, redisPass)
	if err != nil {
		log.Fatalf("Redis 连接失败: %v", err)
	}
	defer rdb.Close()
	connLock := lock.NewConnectionLock(rdb.Client())

	// 为延迟重投单独开一个发布 channel，并开启 publisher confirm
	publishCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("创建发布 channel 失败: %v", err)
	}
	defer publishCh.Close()
	if err := publishCh.Confirm(false); err != nil {
		log.Fatalf("开启 publisher confirm 失败: %v", err)
	}
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

	// 组装消息处理器
	h := worker.New(db, connLock, producer, rdb)

	log.Println("Worker 已启动，等待消息...")

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
