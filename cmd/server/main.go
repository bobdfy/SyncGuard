package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/bobdfy/syncguard/internal/engine"
	"github.com/bobdfy/syncguard/internal/repository"
	"github.com/bobdfy/syncguard/internal/source/mock"
	"github.com/joho/godotenv"
)

func main() {
	// 1. 加载 .env 文件
	godotenv.Load()

	// 2. 解析命令行参数
	var (
		taskName  string
		pageSize  int
		totalRecs int
	)
	flag.StringVar(&taskName, "taskName", "mock_sync", "同步任务名称")
	flag.IntVar(&pageSize, "pageSize", 100, "每页条数")
	flag.IntVar(&totalRecs, "totalRecs", 10000, "数据总条数")
	flag.Parse()

	// 3. 读环境变量 DATABASE_URL
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("databaseURL 未设置")
	}

	// 4. 创建带超时的 context，Ctrl+C 取消
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		<-sigCh
		log.Println("收到中断信息...")
		cancel()
	}()

	// 5. 创建数据库连接池
	db, err := repository.NewDB(ctx, databaseURL)
	if err != nil {
		log.Fatalf("数据库连接失败: %v", err)
	}
	defer db.Close()

	// 6. 创建 Source（Mock 数据源）
	src := mock.NewGenerator(totalRecs)

	// 7. 创建 Destination（PostgreSQL 存储）
	dst := repository.NewSyncedStore(db)

	// 8. 组装引擎
	eng := engine.New(src, dst, pageSize)

	// 9. 跑同步
	if err := eng.Run(ctx, taskName); err != nil {
		log.Printf("同步失败: %v ", err)
		os.Exit(1)
	}
}
