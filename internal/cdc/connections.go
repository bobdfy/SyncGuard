package cdc

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Connection 是 CDC 链路所需的最小连接信息
type Connection struct {
	ID        int
	Name      string
	SourceURL string
	TargetURL string
}

// ListCDCConnections 列出启用了 CDC 的连接
func ListCDCConnections(ctx context.Context, pool *pgxpool.Pool) ([]Connection, error) {
	rows, err := pool.Query(ctx, `
		SELECT id, name, source_url, target_url
		FROM connections
		WHERE cdc_enabled = true
		ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("查询 CDC 连接失败: %w", err)
	}
	defer rows.Close()

	var out []Connection
	for rows.Next() {
		var c Connection
		if err := rows.Scan(&c.ID, &c.Name, &c.SourceURL, &c.TargetURL); err != nil {
			return nil, fmt.Errorf("扫描 CDC 连接失败: %w", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历 CDC 连接失败: %w", err)
	}
	return out, nil
}

// TargetPoolResolver 用中央库构造一个「connectionID → 目标库连接池」的解析器。
func TargetPoolResolver(ctrl *pgxpool.Pool) TargetPoolFunc {
	var mu sync.Mutex
	pools := map[int]*pgxpool.Pool{}

	return func(ctx context.Context, connectionID int) (*pgxpool.Pool, error) {
		mu.Lock()
		p := pools[connectionID]
		mu.Unlock()
		if p != nil {
			return p, nil
		}

		var url string
		if err := ctrl.QueryRow(ctx,
			`SELECT target_url FROM connections WHERE id = $1`, connectionID).Scan(&url); err != nil {
			return nil, fmt.Errorf("查询连接 %d 目标 URL 失败: %w", connectionID, err)
		}
		if url == "" {
			return nil, fmt.Errorf("连接 %d 未配置 target_url", connectionID)
		}

		p, err := pgxpool.New(ctx, url)
		if err != nil {
			return nil, fmt.Errorf("连接 %d 目标库连接失败: %w", connectionID, err)
		}
		if err := p.Ping(ctx); err != nil {
			p.Close()
			return nil, fmt.Errorf("连接 %d 目标库 ping 失败: %w", connectionID, err)
		}

		mu.Lock()
		pools[connectionID] = p
		mu.Unlock()
		slog.Info("已建立目标库连接池", "connection_id", connectionID)
		return p, nil
	}
}
