package engine

import (
	"context"
	"fmt"
	"log"
)

// Engine 同步引擎
//
// 把 Source 的数据搬到 Destination，同时记录断点游标。
// 中途崩溃后重启，从上次的 cursor 继续，不丢数据。
//
// 依赖倒置：Engine 只认 engine.Source 和 engine.Destination 接口，
// 不依赖具体实现（mock.Generator、repository.SyncedStore）。
type Engine struct {
	src      Source
	dst      Destination
	pageSize int
}

// New 构造函数
//
// pageSize 控制每页取多少条，建议 100~500。
func New(src Source, dst Destination, pageSize int) *Engine {
	return &Engine{src, dst, pageSize}
}

// Run 执行同步任务
//
// 流程：
//  1. 从 Destination 读断点 cursor（GetCheckpoint）
//     - 首次运行 cursor=""，从头开始
//     - 崩溃重启 cursor 是上次保存的值，从断点继续
//  2. 创建批次（CreateBatch）记录 status='running'
//  3. 分页循环：
//     a. ctx 检查：select { case <-ctx.Done(): return ctx.Err(); default: }
//     b. src.Fetch(ctx, cursor, pageSize) 拿一页
//     c. records 不空 → dst.Save(ctx, records)、累加 totalCount
//     d. dst.UpdateCheckpoint(ctx, taskName, nextCursor) 更新断点
//     e. hasMore==false → break
//     f. cursor = nextCursor
//  4. dst.CompleteBatch(ctx, batchID, "completed", totalCount)
//
// 断点保证：每页写完立刻更新 cursor，崩溃最多丢一页。
// 幂等保证：Save 内部用 ON CONFLICT DO UPDATE，重复写同一页结果正确。
func (e *Engine) Run(ctx context.Context, taskName string) error {
	//读取断点
	cursor, err := e.dst.GetCheckpoint(ctx, taskName)
	if err != nil {
		return fmt.Errorf("GetCheckpoint is err: %w", err)
	}

	//创建批次
	batchID, err := e.dst.CreateBatch(ctx)
	if err != nil {
		return fmt.Errorf("CreateBatch: %w", err)
	}

	totalCount := 0
	ok := false
	defer func() {
		if !ok {
			_ = e.dst.CompleteBatch(context.Background(), batchID, "failed", totalCount)
		}
	}()

	//分页循环
	for {
		select {
		case <-ctx.Done():
			_ = e.dst.CompleteBatch(context.Background(), batchID, "failed", totalCount)
			return ctx.Err()
		default:
		}

		records, nextCursor, hasMore, err := e.src.Fetch(ctx, cursor, e.pageSize)
		if err != nil {
			return fmt.Errorf("Fetch is err: %w", err)
		}

		if len(records) > 0 {
			if err := e.dst.Save(ctx, records); err != nil {
				return fmt.Errorf("Save: %w", err)
			}
			totalCount += len(records)
		}

		if hasMore == false {
			break
		}
		if err := e.dst.UpdateCheckpoint(ctx, taskName, nextCursor); err != nil {
			return fmt.Errorf("UpdateCheckpoint: %w", err)
		}
		cursor = nextCursor
	}

	if err := e.dst.CompleteBatch(ctx, batchID, "completed", totalCount); err != nil {
		return fmt.Errorf("Complete is err: %w", err)
	}
	ok = true
	log.Printf("同步完成: task=%s, total=%d", taskName, totalCount)
	return nil
}
