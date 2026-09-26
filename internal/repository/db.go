package repository

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5"
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
func NewDB(ctx context.Context, databaseURL string) (*DB, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.New: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pool.Ping: %w", err)
	}
	log.Println("postgres connected")
	return &DB{pool: pool}, nil
}

// Pool 返回底层连接池，给 synced_store 执行 SQL 用
func (db *DB) Pool() *pgxpool.Pool {
	return db.pool
}

// Begin 开启一个事务，供需要「多个操作同生共死」的场景使用（如 outbox）。
// 调用方负责 Commit/Rollback。
func (db *DB) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("DB Begin: %w", err)
	}
	return tx, nil
}

// Close 关闭连接池，释放所有连接
func (db *DB) Close() {
	if db.pool != nil {
		db.pool.Close()
	}
}
