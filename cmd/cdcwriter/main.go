package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/bobdfy/syncguard/internal/cdc"
	"github.com/bobdfy/syncguard/internal/logging"
	"github.com/bobdfy/syncguard/internal/mq"
	"github.com/bobdfy/syncguard/internal/repository"
	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	godotenv.Load()
	logging.Init()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		slog.Info("收到退出信息")
		cancel()
	}()

	// 目标数据库
	targetDB, err := repository.NewDB(ctx, os.Getenv("TARGET_DATABASE_URL"))
	if err != nil {
		slog.Error("目标数据库连接失败", "err", err)
		os.Exit(1)
	}
	defer targetDB.Close()

	conn, ch, err := mq.Connect(os.Getenv("RABBITMQ_URL"))
	if err != nil {
		slog.Error("RabbitMQ 连接失败", "err", err)
		os.Exit(1)
	}
	defer conn.Close()
	defer ch.Close()

	consumer, err := mq.NewConsumer(ch, mq.CDCQueueName)
	if err != nil {
		slog.Error("创建 CDC 消费者失败", "err", err)
		os.Exit(1)
	}
	msgs, err := consumer.Consume()
	if err != nil {
		slog.Error("订阅 CDC 队列失败", "err", err)
		os.Exit(1)
	}

	applier := cdc.NewApplier(targetDB.Pool())

	slog.Info("CDC 消费者已启动", "queue", mq.CDCQueueName)

	for {
		select {
		case <-ctx.Done():
			slog.Info("CDC 消费者已停止")
			return
		case msg, ok := <-msgs:
			if !ok {
				slog.Warn("消息 channel 已关闭")
				return
			}
			handle(msg, applier)
		}
	}
}

func handle(msg amqp.Delivery, applier *cdc.Applier) {
	var ev cdc.Event
	if err := json.Unmarshal(msg.Body, &ev); err != nil {
		slog.Error("解析 CDC 事件失败，丢弃", "err", err)
		msg.Nack(false, false) // 解析失败是永久错误，重试无意义
		return
	}

	if err := applier.Apply(context.Background(), ev); err != nil {
		slog.Error("应用 CDC 事件失败，丢弃", "lsn", ev.LSN, "operation", ev.Operation, "table", ev.Schema+"."+ev.Table, "err", err)
		msg.Nack(false, false)
		return
	}

	slog.Info("已应用", "lsn", ev.LSN, "operation", ev.Operation, "table", ev.Schema+"."+ev.Table)

	msg.Ack(false)
}
