package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/bobdfy/syncguard/internal/cdc"
	"github.com/bobdfy/syncguard/internal/logging"
	"github.com/bobdfy/syncguard/internal/mq"
	"github.com/bobdfy/syncguard/internal/repository"
	"github.com/joho/godotenv"
)

func main() {
	godotenv.Load()
	logging.Init()

	replay := flag.Bool("replay", false, "一次性补投：把 CDC 死信队列的消息发回主队列后退出")
	flag.Parse()

	conn, ch, err := mq.Connect(os.Getenv("RABBITMQ_URL"))
	if err != nil {
		slog.Error("RabbitMQ 连接失败", "err", err)
		os.Exit(1)
	}
	defer conn.Close()
	defer ch.Close()

	// 一次性补投：只走死信队列，不需要目标库
	if *replay {
		n, err := cdc.Replay(ch)
		if err != nil {
			slog.Error("补投失败", "err", err)
			os.Exit(1)
		}
		slog.Info("补投完成", "total", n)
		return
	}

	// 常驻消费：连目标库，消费 CDC 队列应用到镜像表
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	targetDB, err := repository.NewDB(ctx, os.Getenv("TARGET_DATABASE_URL"))
	if err != nil {
		slog.Error("目标数据库连接失败", "err", err)
		os.Exit(1)
	}
	defer targetDB.Close()

	if err := cdc.Serve(ctx, ch, targetDB.Pool()); err != nil {
		slog.Error("CDC 消费异常退出", "err", err)
		os.Exit(1)
	}
}
