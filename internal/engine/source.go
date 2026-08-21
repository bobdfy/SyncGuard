package engine

import (
	"context"

	"github.com/bobdfy/syncguard/internal/model"
)

// Source 数据源接口
// 所有外部数据源（Mock、GitHub、GitLab 等）都要实现这个接口
// 同步引擎只依赖接口，不关心具体实现
type Source interface {
	// Fetch 从 cursor 位置取一页数据
	// cursor 为空字符串表示从头开始
	// 返回：
	//   - records: 本页记录切片
	//   - nextCursor: 下一页游标，空字符串表示没有下一页
	//   - hasMore: 是否还有更多数据
	Fetch(ctx context.Context, cursor string, limit int) (
		records []model.Record,
		nextCursor string,
		hasMore bool,
		err error)

	// Close 释放数据源持有的资源（连接池等）。
	// 无资源的实现（mock/github）返回 nil；调用方应在用完后 defer 关闭。
	Close() error
}
