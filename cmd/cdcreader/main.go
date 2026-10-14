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

	dsn := os.Getenv("DATABASE_URL")
	publication := "syncguard_pub"
	table := "documents" // 快照/复制的表，与 publication 覆盖的表一致

	// -snapshot：一次性存量快照（建槽钉切点 + 搬存量到目标库后退出）
	snapshot := flag.Bool("snapshot", false, "一次性存量快照：建槽钉切点 + 搬存量到目标库后退出")
	slotName := flag.String("slot", "syncguard_slot", "复制槽名")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigCh
		slog.Info("收到退出信号，准备关闭", "signal", sig.String())
		cancel()
	}()

	// reader 写 cdc_events，dispatcher 读 + 标记，快照写断点，共用一个池
	db, err := repository.NewDB(ctx, dsn)
	if err != nil {
		slog.Error("数据库连接失败", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	store := cdc.NewEventStore(db.Pool(), *slotName)

	// 快照模式：一次性存量搬运，完成后退出；不接 MQ、不流式。
	if *snapshot {
		targetDSN := os.Getenv("TARGET_DATABASE_URL")
		if targetDSN == "" {
			slog.Error("TARGET_DATABASE_URL 未配置")
			os.Exit(1)
		}
		if err := cdc.Snapshot(ctx, dsn, targetDSN, *slotName, table, store); err != nil {
			slog.Error("存量快照失败", "err", err)
			os.Exit(1)
		}
		slog.Info("存量快照完成，退出")
		return
	}

	// MQ 连接：dispatcher 把事件投到 CDC 队列
	conn, ch, err := mq.Connect(os.Getenv("RABBITMQ_URL"))
	if err != nil {
		slog.Error("RabbitMQ 连接失败", "err", err)
		os.Exit(1)
	}
	defer conn.Close()
	defer ch.Close()

	// dispatcher 单独开一个发布 channel 并开启 publisher confirm
	// 消息被 Broker 确认接收后才标 processed_at
	publishCh, err := conn.Channel()
	if err != nil {
		slog.Error("创建发布 channel 失败", "err", err)
		os.Exit(1)
	}
	defer publishCh.Close()
	// noWait=false：同步等待进入 confirm 模式，后续 PublishWithDeferredConfirm 才可用
	if err := publishCh.Confirm(false); err != nil {
		slog.Error("开启 publisher confirm 失败", "err", err)
		os.Exit(1)
	}

	producer := mq.NewProducer(publishCh, mq.CDCExchangeName, mq.CDCRoutingKey)

	// dispatcher：轮询 cdc_events 未分发事件 → 投 MQ（异步 goroutine）
	dispatcher := cdc.NewDispatcher(producer, store)
	go dispatcher.Start(ctx)

	// reader：解码 WAL → 写 cdc_events（阻塞主流程）
	reader, err := cdc.NewReader(ctx, dsn, *slotName, publication, store)
	if err != nil {
		slog.Error("创建 CDC Reader 失败", "err", err)
		os.Exit(1)
	}
	defer reader.Close()

	slog.Info("CDC Reader 启动", "slot", *slotName, "publication", publication)

	if err := reader.Run(ctx); err != nil {
		slog.Error("CDC Reader 异常退出", "err", err)
		os.Exit(1)
	}

	slog.Info("CDC Reader 正常退出")
}
