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

// NewTableDestination 根据目标连接 ID 创建数据库镜像目标（engine.TableDestination）。
//
// 与 NewDestination 的区别：返回的是「原表镜像」目标——按源表结构建同构表、
// 列对列写入，而不是写 (id,version,updated_at,data) 四列信封表。用于数据库源的镜像轨道。
func NewTableDestination(ctx context.Context, connStore *repository.ConnectionStore, targetConnectionID int, syncContent string) (engine.TableDestination, error) {
	conn, err := connStore.GetByID(ctx, targetConnectionID)
	if err != nil {
		return nil, fmt.Errorf("查询目标连接配置失败: %w", err)
	}

	switch conn.SourceType {
	case "postgres":
		// 目标表名 = 源表名（复用 syncContent 的 "table[:pk]" 格式，只取表名）。
		table, _, _ := strings.Cut(syncContent, ":")
		return postgres.NewTableDestination(ctx, conn.SourceURL, table)

	default:
		return nil, fmt.Errorf("不支持的目标类型: %w: %s", engine.ErrNonRetryable, conn.SourceType)
	}
}
