package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bobdfy/syncguard/internal/config"
	"github.com/bobdfy/syncguard/internal/handler"
	"github.com/bobdfy/syncguard/internal/logging"
	"github.com/bobdfy/syncguard/internal/middleware"
	"github.com/bobdfy/syncguard/internal/mq"
	"github.com/bobdfy/syncguard/internal/reconciliation"
	"github.com/bobdfy/syncguard/internal/repository"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"golang.org/x/time/rate"
)

// fatal 打一条结构化错误日志后以非零码退出（slog 没有 Fatal，自己包一层）。
func fatal(msg string, args ...any) {
	slog.Error(msg, args...)
	os.Exit(1)
}

func main() {
	godotenv.Load()

	// 初始化结构化日志（放在 godotenv 之后，.env 里的 LOG_LEVEL 才能生效）
	logging.Init()

	cfg, err := config.Load(true, false)
	if err != nil {
		fatal(err.Error())
	}

	databaseURL := cfg.DatabaseURL

	// JWT 签名密钥：从环境变量注入，杜绝硬编码。空值直接拒绝启动。
	middleware.SetJWTSecret(cfg.JWTSecret)

	db, err := repository.NewDB(context.Background(), databaseURL)
	if err != nil {
		fatal("数据库连接失败", "err", err)
	}
	defer db.Close()

	// 创建 Store
	userStore := repository.NewUserStore(db)
	connStore := repository.NewConnectionStore(db)
	jobStore := repository.NewJobStore(db)
	outboxStore := repository.NewOutboxStore(db)

	// 创建对账引擎
	reconciler := reconciliation.NewReconciler(db, connStore)

	// 连接 RabbitMQ，创建 Producer
	rabbitURL := cfg.RabbitURL

	// 拨号 + 声明拓扑（交换机、队列、死信队列）
	conn, ch, err := mq.Connect(rabbitURL)
	if err != nil {
		fatal("RabbitMQ 连接失败", "err", err)
	}
	defer conn.Close()
	defer ch.Close()

	// 创建 Producer，往主交换机发消息
	producer := mq.NewProducer(ch, mq.ExchangeName, mq.RoutingKey)

	// 创建 Handler
	authHandler := handler.NewAuthHandler(userStore)
	connHandler := handler.NewConnectionHandler(connStore)

	jobHandler := handler.NewJobHandler(jobStore, connStore, db, producer, reconciler, outboxStore)

	r := gin.Default()

	// 不信任任何代理：ClientIP 只用 TCP 连接的 RemoteAddr。
	// 否则客户端伪造一个 X-Forwarded-For 头就能换个 IP 桶，按 IP 的限流直接失效。
	if err := r.SetTrustedProxies(nil); err != nil {
		fatal("设置可信代理失败", "err", err)
	}

	// 根 ctx：进程退出时取消，所有带生命周期的组件（限流的清理 goroutine 等）随之停止
	rootCtx, cancelRoot := context.WithCancel(context.Background())
	defer cancelRoot()

	// 登录/注册限流：每 12 秒补 1 个令牌（≈5 次/分钟），桶满可突发 5 次
	loginLimiter := middleware.NewIPRateLimiter(rootCtx, rate.Every(12*time.Second), 5)
	defer loginLimiter.Close()

	// ========== 公开 API ==========
	r.POST("/api/register", loginLimiter.Middleware(), authHandler.Register)
	r.POST("/api/login", loginLimiter.Middleware(), authHandler.Login)

	// ========== 需要认证的 API ==========
	api := r.Group("/api")
	api.Use(middleware.AuthRequired())
	{
		api.GET("/logout", authHandler.Logout)

		// 数据源 CRUD
		api.GET("/connections", connHandler.ListConnections)
		api.POST("/connections", connHandler.CreateConnection)
		api.PUT("/connections/:id", connHandler.UpdateConnection)
		api.DELETE("/connections/:id", connHandler.DeleteConnection)

		// 同步任务 CRUD
		api.GET("/jobs", jobHandler.ListJobs)
		api.POST("/jobs", jobHandler.CreateJob)
		api.GET("/jobs/:id", jobHandler.GetJob)
		api.DELETE("/jobs/:id", jobHandler.DeleteJob)

		api.POST("/jobs/:id/run", jobHandler.RunJob)
		api.POST("/jobs/:id/reconcile", jobHandler.Reconcile) // V3：对账

		api.GET("/records", jobHandler.ListRecords)
	}

	// ========== 静态文件（前端页面） ==========
	r.Static("/static", "./web")
	r.StaticFile("/", "./web/login.html")

	// 读取端口（提到 goroutine 外，供日志打印使用）
	port := cfg.Port

	srv := &http.Server{Addr: ":" + port, Handler: r}
	go func() {
		// 监听
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fatal("服务启动失败", "err", err)
		}
	}()

	// 带缓冲的通道
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		slog.Warn("优雅退出超时，强制关闭", "err", err)
	}
	slog.Info("Server 已优雅退出")
}
