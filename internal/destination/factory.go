package destination

import (
	"context"
	"fmt"
	"strings"

	"github.com/bobdfy/syncguard/internal/destination/postgres"
	"github.com/bobdfy/syncguard/internal/engine"
	"github.com/bobdfy/syncguard/internal/repository"
)

// NewDestination 根据目标连接 ID 创建外部目标。
// 注意：targetConnectionID == 0 表示「内部存储」，由调用方自己处理，这里只负责外部目标。
//
// 流程：
//  1. connStore.GetByID(ctx, targetConnectionID) 查连接配置
//  2. switch conn.SourceType：
//     "postgres" → postgres.NewDestination(ctx, conn.SourceURL, table)
//     default    → 错误「不支持的目标类型」
func NewDestination(ctx context.Context, connStore *repository.ConnectionStore, targetConnectionID int, syncContent string) (engine.Destination, error) {
	conn, err := connStore.GetByID(ctx, targetConnectionID)
	if err != nil {
		return nil, fmt.Errorf("查询目标连接配置失败: %w", err)
	}

	switch conn.SourceType {
	case "postgres":
		// syncContent 与源端共用 "table[:pk]" 格式，这里只取冒号前的表名部分。
		table, _, _ := strings.Cut(syncContent, ":")
		return postgres.NewDestination(ctx, conn.SourceURL, table)

	default:
		return nil, fmt.Errorf("不支持的目标类型: %w: %s", engine.ErrNonRetryable, conn.SourceType)
	}
}
