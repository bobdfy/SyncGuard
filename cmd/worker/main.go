package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/bobdfy/syncguard/internal/config"
	"github.com/bobdfy/syncguard/internal/lock"
	"github.com/bobdfy/syncguard/internal/logging"
	"github.com/bobdfy/syncguard/internal/mq"
	"github.com/bobdfy/syncguard/internal/redisclient"
	"github.com/bobdfy/syncguard/internal/repository"
	"github.com/bobdfy/syncguard/internal/worker"
	"github.com/joho/godotenv"
)

// fatal 打一条结构化错误日志后以非零码退出（slog 没有 Fatal，自己包一层）。
func fatal(msg string, args ...any) {
	slog.Error(msg, args...)
	os.Exit(1)
}

func main() {
	// 加载 .env
	godotenv.Load()

	// 初始化结构化日志（放在 godotenv 之后，.env 里的 LOG_LEVEL 才能生效）
	logging.Init()

	replay := flag.Bool("replay", false, "一次性补投：把 jobs 死信队列的消息发回主队列后退出")
	flag.Parse()

	cfg, err := config.Load(false, true)
	if err != nil {
		fatal(err.Error())
	}

	// 连接 RabbitMQ（放最前：-replay 只走死信队列，不依赖 DB/Redis）
	conn, ch, err := mq.Connect(cfg.RabbitURL)
	if err != nil {
		fatal("RabbitMQ 连接失败", "err", err)
	}
	defer conn.Close()
	defer ch.Close()

	// 一次性补投：只走死信队列，不需要 DB/Redis
	if *replay {
		n, err := worker.ReplayJobs(ch)
		if err != nil {
			fatal("补投失败", "err", err)
		}
		slog.Info("补投完成", "total", n)
		return
	}

	// 连接 PostgreSQL
	db, err := repository.NewDB(context.Background(), cfg.DatabaseURL)
	if err != nil {
		fatal("数据库连接失败", "err", err)
	}
	defer db.Close()

	// 连接 Redis，创建连接级分布式锁
	rdb, err := redisclient.NewClient(context.Background(), cfg.RedisAddr, cfg.RedisPass)
	if err != nil {
		fatal("Redis 连接失败", "err", err)
	}
	defer rdb.Close()

	//创建一个连接级别的分布式锁，用于防止同一个连接被多个 Worker 同时使用
	connLock := lock.NewConnectionLock(rdb.Client())

	// 为延迟重投单独开一个发布 channel，并开启 publisher confirm
	publishCh, err := conn.Channel()
	if err != nil {
		fatal("创建发布 channel 失败", "err", err)
	}
	defer publishCh.Close() //延迟关闭

	// 确保消息被 Broker 成功接收后才返回 ACK
	// false 表示不需要同步等待所有确认，性能较好
	if err := publishCh.Confirm(false); err != nil {
		fatal("开启 publisher confirm 失败", "err", err)
	}

	// 创建生产者实例，指定交换器、路由键（这些常量在 mq 包中定义）
	producer := mq.NewProducer(publishCh, mq.ExchangeName, mq.RoutingKey)

	// 创建 Consumer（内部设 Qos(prefetch=1)，一次只取一条）
	consumer, err := mq.NewConsumer(ch, mq.QueueName)
	if err != nil {
		fatal("创建 Consumer 失败", "err", err)
	}

	// 订阅队列，拿到消息 channel
	msgs, err := consumer.Consume()
	if err != nil {
		fatal("订阅队列失败", "err", err)
	}

	//组装outbox搬运工：扫描outbox未发送的信息 --> 发送到RabbitMQ
	//创建 outbox 数据存储层实例，用于操作 outbox 表
	outboxStore := repository.NewOutboxStore(db)

	//负责定期扫描 outbox 表，将 pending 消息通过 producer 发送到 RabbitMQ
	dispatcher := worker.NewDispatcher(producer, outboxStore)

	// 组装消息处理器
	// 创建 worker.Handler，负责处理从队列中消费到的每条消息（即执行任务）
	h := worker.New(db, connLock, rdb, outboxStore)

	slog.Info("Worker 已启动，等待消息...")

	//启动搬运工：独立ctx， worker退出时一并取消
	dispatcherCtx, cancelDispatcher := context.WithCancel(context.Background())
	defer cancelDispatcher()

	// 启动搬运工，它会每隔一段时间扫描 outbox 表，发送未发送的消息
	go dispatcher.Start(dispatcherCtx)

	// 优雅退出：Ctrl+C 时退出循环
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	// 消息循环：阻塞等待消息或退出信号
	for {
		select {
		case <-quit:
			slog.Info("正在关闭 Worker...")
			return

		case msg, ok := <-msgs:
			if !ok {
				slog.Warn("消息 channel 已关闭")
				return
			}
			h.Handle(msg)
		}
	}
}
