package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
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

	// 每个源库都用同一个 publication 名；表清单由 publication 决定（自动抓取，无需逐个配置）。
	const (
		publication = "syncguard_pub"
	)

	snapshot := flag.Bool("snapshot", false, "一次性存量快照：对每个启用 CDC 的连接建槽钉切点 + 搬存量后退出")
	connectionID := flag.Int("connection", 0, "只处理指定连接（0=全部启用 CDC 的连接）")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 中央库：控制面。存 connections / cdc_events / cdc_checkpoints。
	ctrlDB, err := repository.NewDB(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		slog.Error("中央数据库连接失败", "err", err)
		os.Exit(1)
	}
	defer ctrlDB.Close()

	conns, err := cdc.ListCDCConnections(ctx, ctrlDB.Pool())
	if err != nil {
		slog.Error("读取 CDC 连接失败", "err", err)
		os.Exit(1)
	}

	// -connection N 只跑单个连接
	if *connectionID != 0 {
		var one []cdc.Connection
		for _, c := range conns {
			if c.ID == *connectionID {
				one = []cdc.Connection{c}
				break
			}
		}
		conns = one
	}
	if len(conns) == 0 {
		slog.Warn("没有启用 CDC 的连接，等待中")
		<-ctx.Done()
		return
	}

	// 快照模式：每个连接一次性搬存量，完成后退出；不接 MQ、不流式。
	if *snapshot {
		failed := 0
		for _, c := range conns {
			slot := fmt.Sprintf("syncguard_slot_%d", c.ID)
			store := cdc.NewEventStore(ctrlDB.Pool(), c.ID, slot)
			if err := cdc.Snapshot(ctx, c.SourceURL, c.TargetURL, slot, publication, store); err != nil {
				// 坏连接跳过，其余继续，最后汇总非零退出（与流式模式策略一致）
				slog.Error("存量快照失败，跳过该连接", "connection_id", c.ID, "err", err)
				failed++
			}
		}
		if failed > 0 {
			slog.Error("存量快照部分失败", "failed", failed, "total", len(conns))
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

	// dispatcher：全局扫 cdc_events 未分发事件（跨所有连接）→ 投 MQ
	dispatcher := cdc.NewDispatcher(producer, cdc.NewDispatchStore(ctrlDB.Pool()))
	go dispatcher.Start(ctx)

	// readerErr：任一 reader 异常退出时写入，主协程据此非零退出。
	// 有缓冲，reader 先写错误再 cancel 根 ctx，保证主协程能读到。
	readerErr := make(chan error, len(conns))

	// 每个连接一个 reader：各读各的源库 WAL，写中央 cdc_events + 自己的断点
	started := 0
	for _, c := range conns {
		c := c
		slot := fmt.Sprintf("syncguard_slot_%d", c.ID)
		store := cdc.NewEventStore(ctrlDB.Pool(), c.ID, slot)
		reader, err := cdc.NewReader(ctx, c.SourceURL, slot, publication, store)
		if err != nil {
			// 单个坏连接不拖死全部：跳过并告警，其余连接继续跑。
			slog.Error("创建 CDC Reader 失败，跳过该连接", "connection_id", c.ID, "err", err)
			continue
		}
		started++
		go func() {
			defer reader.Close()
			slog.Info("CDC Reader 启动", "connection_id", c.ID, "slot", slot, "publication", publication)
			if err := reader.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				// 复制槽已建、WAL 会持续堆积：不能只打日志装死，整体退出交给 supervisor 重启。
				slog.Error("CDC Reader 异常退出", "connection_id", c.ID, "err", err)
				readerErr <- err
				stop()
			}
		}()
	}

	if started == 0 {
		slog.Error("所有 CDC Reader 都创建失败，进程退出交给 supervisor 重启")
		os.Exit(1)
	}

	slog.Info("CDC Reader 已启动", "count", started)

	<-ctx.Done()

	// 区分退出原因：正常收到信号（readerErr 空）还是 reader 异常退出（readerErr 有值）。
	select {
	case err := <-readerErr:
		slog.Error("CDC Reader 失败，进程整体退出", "err", err)
		os.Exit(1)
	default:
		slog.Info("CDC Reader 正常退出")
	}
}
