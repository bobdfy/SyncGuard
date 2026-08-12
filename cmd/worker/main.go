package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"

	"github.com/bobdfy/syncguard/internal/engine"
	"github.com/bobdfy/syncguard/internal/mq"
	"github.com/bobdfy/syncguard/internal/repository"
	"github.com/bobdfy/syncguard/internal/source"
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

			// 7a. 反序列化 → JobMessage
			// 格式损坏则丢弃到死信队列等人工排查
			var jobMsg mq.JobMessage
			err := json.Unmarshal(msg.Body, &jobMsg)
			if err != nil {
				log.Println("解析JSON消息失败")
				msg.Nack(false, false)
				continue
			}

			log.Printf("[Worker] 收到消息 job_id=%d task_name=%s", jobMsg.JobID, jobMsg.TaskName)

			// 7b. 查 DB 获取任务
			jobStore := repository.NewJobStore(db)
			// TODO: 填空1 — 创建 ConnectionStore（提示：看 factory.go NewSource 的第二个参数）
			ConnectionStore := repository.NewConnectionStore(db)
			job, err := jobStore.GetByID(context.Background(), jobMsg.JobID)
			if err != nil {
				log.Printf("[Worker] 查询任务 %d 失败: %v", jobMsg.JobID, err)
				msg.Nack(false, false)
				continue
			}

			// 7c. 幂等保护：已完成的任务直接 ACK 跳过
			if job.Status == "completed" {
				log.Printf("[Worker] 任务已执行,跳过")
				msg.Ack(false)
				continue
			}

			// 7d. 组装引擎：Mock 数据源 + JobDestination + 每页 200 条
			// TODO: 填空2 — 调 source.NewSource 创建数据源
			src, err := source.NewSource(context.Background(), db, ConnectionStore, jobMsg.ConnectionID)
			if err != nil {
				log.Printf("[Worker] 创建数据源失败: %v", err)
				msg.Nack(false, false)
				continue
			}
			syncedStore := repository.NewSyncedStore(db)
			dst := repository.NewJobDestination(syncedStore, jobStore, jobMsg.JobID)
			eng := engine.New(src, dst, 200)

			// 7e. 执行 → 成功 ACK / 失败 NACK 进死信队列
			log.Printf("[Worker] 开始执行 job_id=%d task_name=%s", jobMsg.JobID, jobMsg.TaskName)
			err = eng.Run(context.Background(), jobMsg.TaskName)
			if err != nil {
				log.Printf("[Worker] 任务(id=%d name=%s) 执行失败: %v", jobMsg.JobID, jobMsg.TaskName, err)
				msg.Nack(false, false)
				continue
			}
			log.Println("[Worker] 执行完成")
			msg.Ack(false)
		}
	}
}
