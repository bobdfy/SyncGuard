package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/bobdfy/syncguard/internal/handler"
	"github.com/bobdfy/syncguard/internal/middleware"
	"github.com/bobdfy/syncguard/internal/mq"
	"github.com/bobdfy/syncguard/internal/reconciliation"
	"github.com/bobdfy/syncguard/internal/repository"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

func main() {
	godotenv.Load()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL 未设置")
	}

	// JWT 签名密钥：从环境变量注入，杜绝硬编码。空值直接拒绝启动。
	if err := middleware.SetJWTSecret(os.Getenv("JWT_SECRET")); err != nil {
		log.Fatalf("JWT_SECRET: %v", err)
	}

	db, err := repository.NewDB(context.Background(), databaseURL)
	if err != nil {
		log.Fatalf("数据库连接失败: %v", err)
	}
	defer db.Close()

	// 创建 Store
	userStore := repository.NewUserStore(db)
	connStore := repository.NewConnectionStore(db)
	jobStore := repository.NewJobStore(db)

	// V3：创建对账引擎
	reconciler := reconciliation.NewReconciler(db, connStore)

	// ========== V2：连接 RabbitMQ，创建 Producer ==========
	// 从 .env 读取 RabbitMQ 地址
	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		log.Fatal("RABBITMQ_URL 未设置")
	}

	// 拨号 + 声明拓扑（交换机、队列、死信队列）
	conn, ch, err := mq.Connect(rabbitURL)
	if err != nil {
		log.Fatalf("RabbitMQ 连接失败: %v", err)
	}
	defer conn.Close()
	defer ch.Close()

	// 创建 Producer，往主交换机发消息
	producer := mq.NewProducer(ch, mq.ExchangeName, mq.RoutingKey)

	// 创建 Handler（V2：JobHandler 多了 Producer 参数）
	authHandler := handler.NewAuthHandler(userStore)
	connHandler := handler.NewConnectionHandler(connStore)
	jobHandler := handler.NewJobHandler(jobStore, connStore, db, producer, reconciler) // V3：加了对账引擎

	r := gin.Default()

	// ========== 公开 API ==========
	r.POST("/api/register", authHandler.Register)
	r.POST("/api/login", authHandler.Login)

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
	port := os.Getenv("PORT")
	if port == "" {
		port = "9090"
	}

	// 启动 HTTP 服务
	go func() {
		if err := r.Run(":" + port); err != nil {
			log.Fatalf("服务启动失败: %v", err)
		}
	}()
	log.Printf("SyncGuard 已启动: http://localhost:%s", port)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	log.Println("正在关闭...")
}
