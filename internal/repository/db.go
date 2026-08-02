package repository

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DB 封装 pgxpool 连接池
//
// 只负责连接管理（创建、验证、关闭），不写 SQL。
// SQL 操作放在 synced_store.go 里，保证职责单一。
type DB struct {
	pool *pgxpool.Pool
}

// NewDB 创建连接池并验证连接是否可用
//
// 参数 databaseURL 格式：
//
//	postgres://user:pass@host:port/dbname?sslmode=disable
//
// 使用 pgxpool 而不是 database/sql：
//   - pgx 是纯 Go 原生 PG 驱动，不用 CGO
//   - pgxpool 自带连接池管理，不需要额外配置
//   - 支持 PG 特有功能：COPY、LISTEN/NOTIFY、advisory lock
//
// 返回 error 而不是 panic，让调用方决定怎么处理
func NewDB(ctx context.Context, databaseURL string) (*DB, error) {
	// TODO:
	// 1. 用 pgxpool.New(ctx, databaseURL) 创建连接池
	//    - 这一步会解析 URL，但不会真正连数据库
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.New: %w", err)
	}
	// 2. 用 pool.Ping(ctx) 验证连接
	//    - 网络不通、密码错误在这里暴露
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pool.Ping: %w", err)
	}
	// 3. 如果 Ping 失败，pool.Close() 释放资源再返回 error
	log.Println("postgres connected")
	// 4. 成功后返回 &DB{pool: pool}
	return &DB{pool: pool}, nil
}

// Pool 返回底层连接池，给 synced_store 执行 SQL 用
//
// 不直接暴露 pool 字段，用方法访问：
//   - 外面只能读不能改
//   - 后续加日志、metrics 可以在这里拦截
func (db *DB) Pool() *pgxpool.Pool {
	return db.pool
}

// Close 关闭连接池，释放所有连接
//
// 调用时机：
//   - 程序正常退出前（defer db.Close()）
//   - 连接失败后的清理
//
// Close 之后不能再执行任何 SQL，否则 panic
func (db *DB) Close() {
	// TODO:
	// 1. 如果 pool 不为 nil，调用 pool.Close()
	// // 2. pool.Close() 会等所有正在执行的查询完成再关
	if db.pool != nil {
		db.pool.Close()
	}

}
