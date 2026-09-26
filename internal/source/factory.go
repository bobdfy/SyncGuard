package source

import (
	"context"
	"fmt"
	"strings"

	"github.com/bobdfy/syncguard/internal/engine"
	"github.com/bobdfy/syncguard/internal/repository"
	"github.com/bobdfy/syncguard/internal/source/github"
	"github.com/bobdfy/syncguard/internal/source/mock"
	"github.com/bobdfy/syncguard/internal/source/postgres"
)

/*
NewSource 根据 connectionID 查 DB，按 source_type 创建对应的 Source 实现。

这就是"工厂模式"：调用方具只拿到 engine.Source 接口，不关心体是 mock 还是 github。
以后加 GitLab、Jira 等新数据源，只需：
 1. 写一个新包（如 internal/source/gitlab/source.go），实现 Fetch 方法
 2. 在这里加一个 case
    Engine 一行都不用改——这就是依赖倒置的价值。

流程：
 1. 查 connections 表 → 拿到 source_type + source_url
 2. switch source_type 创建对应的 Source

source_url 的格式取决于 source_type：
  - mock:  不需要（用不到）
  - github: "owner/repo"，如 "golang/go"、"torvalds/linux"
*/
func NewSource(ctx context.Context, db *repository.DB, connectionStore *repository.ConnectionStore, connectionID int, syncContent string) (engine.Source, error) {
	// 1. 查 connections 表
	conn, err := connectionStore.GetByID(ctx, connectionID)
	if err != nil {
		return nil, fmt.Errorf("查询连接配置失败: %w", err)
	}

	// 2. 根据 source_type 选择实现
	switch conn.SourceType {

	case "mock":
		return mock.NewGenerator(1000), nil

	case "github":
		parts := strings.SplitN(conn.SourceURL, "/", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("GitHub source_url 格式错误，需要 owner/repo: %w(实际: %s)", engine.ErrNonRetryable, conn.SourceURL)
		}
		owner := parts[0]
		repo := parts[1]
		return github.NewSource(owner, repo), nil

	case "postgres":
		return postgres.NewSource(ctx, conn.SourceURL, syncContent)

	default:
		return nil, fmt.Errorf("不支持的数据源类型: %w: %s", engine.ErrNonRetryable, conn.SourceType)
	}
}
