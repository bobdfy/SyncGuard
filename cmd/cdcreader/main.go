package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/bobdfy/syncguard/internal/cdc"
	"github.com/bobdfy/syncguard/internal/logging"
	"github.com/bobdfy/syncguard/internal/mq"
	"github.com/joho/godotenv"
)

func main() {
	godotenv.Load()
	logging.Init()

	dsn := os.Getenv("DATABASE_URL")
	slotName := "syncguard_slot"
	publication := "syncguard_pub"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigCh
		slog.Info("收到退出信号，准备关闭", "signal", sig.String())
		cancel()
	}()

	// 连接MQ
	conn, ch, err := mq.Connect(os.Getenv("RABBITMQ_URL"))
	if err != nil {
		slog.Error("RabbitMQ 连接失败", "err", err)
		os.Exit(1)
	}
	defer conn.Close()
	defer ch.Close()

	producer := mq.NewProducer(ch, mq.CDCExchangeName, mq.CDCRoutingKey)
	hanlder := cdc.NewMQHanlder(producer)

	reader, err := cdc.NewReader(ctx, dsn, slotName, publication, hanlder)
	if err != nil {
		slog.Error("创建 CDC Reader 失败", "err", err)
		os.Exit(1)
	}
	defer reader.Close()

	slog.Info("CDC Reader 启动", "slot", slotName, "publication", publication)

	if err := reader.Run(ctx); err != nil {
		slog.Error("CDC Reader 异常退出", "err", err)
		os.Exit(1)
	}

	slog.Info("CDC Reader 正常退出")

}
