package mock

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/bobdfy/syncguard/internal/engine"
	"github.com/bobdfy/syncguard/internal/model"
)

// Generator 模拟数据源
// 预生成指定数量的记录存内存，实现 engine.Source 接口
// 用于 V0 阶段验证同步引擎的核心逻辑
type Generator struct {
	records []model.Record

	recordsFast map[string]int // 保存记录 ID 对应的切片下标，用于快速定位
}

// 编译期检查：Generator 实现了 engine.Source 接口
var _ engine.Source = (*Generator)(nil)

// NewGenerator 预生成 count 条记录
//
// 规则：
//   - ID 格式 "record_0001" ~ "record_9999"
//   - Version 从 1 开始，随机递增（模拟真实更新）
//   - UpdatedAt 从 2026-01-01 开始每条约 +30s（模拟时间推进）
//   - 约 5% 的记录相邻时间戳相同（模拟真实并发写入场景）
func NewGenerator(count int) *Generator {
	g := &Generator{
		records:     make([]model.Record, count),
		recordsFast: make(map[string]int, count),
	}
	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range count {
		rec := model.Record{
			ID:      fmt.Sprintf("record_%04d", i+1),
			Version: 1 + rand.Intn(5),
			Data:    json.RawMessage(`{}`),
		}
		if i == 0 {
			rec.UpdatedAt = baseTime
		} else if i%20 == 0 {
			rec.UpdatedAt = g.records[i-1].UpdatedAt
		} else {
			rec.UpdatedAt = baseTime.Add(time.Duration(i) * 30 * time.Second)
		}
		g.records[i] = rec
		g.recordsFast[rec.ID] = i
	}
	return g
}

// Fetch 实现 engine.Source 接口
//
// 基于 cursor（最后一条记录 ID）定位，返回接下来的 limit 条
// cursor 为空字符串表示从头开始
//
// 返回：
//   - records: 本页记录切片
//   - nextCursor: 下一页游标（最后一条记录的 ID），无更多数据时为空
//   - hasMore: 是否还有更多数据
//   - err: 错误
func (g *Generator) Fetch(ctx context.Context, cursor string, limit int) ([]model.Record, string, bool, error) {
	// 检查任务是否收到取消或超时信号
	select {
	case <-ctx.Done():
		return nil, "", false, ctx.Err()
	default:
	}

	// 检查单次读取数量是否合法
	if limit <= 0 {
		return nil, "", false, fmt.Errorf("limit必须大于0")
	}

	// 如果还有更多数据，将本页最后一条记录的 ID 作为下一页游标
	start := 0
	if cursor != "" {
		index, ok := g.recordsFast[cursor]
		if !ok {
			return nil, "", false, fmt.Errorf("cursor无效:%s", cursor)
		}
		start = index + 1
	}

	//判断是否已经读完
	if start >= len(g.records) {
		return []model.Record{}, "", false, nil
	}

	//计算这一页读取到哪里。
	end := min(start+limit, len(g.records))

	//截取这一页的数据
	page := g.records[start:end]

	//判断后面还有没有数据。
	hasMore := end < len(g.records)

	//默认没有下一页 cursor
	nextCursor := ""

	//如果有hasMore，说明还有数据，再取这一页最后一条记录的ID作为开始
	if hasMore {
		nextCursor = page[len(page)-1].ID
	}

	// 9. 返回
	return page, nextCursor, hasMore, nil
}
