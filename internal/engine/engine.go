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
	return &Engine{
		src:      src,
		dst:      dst,
		pageSize: pageSize}
}

// Run 执行同步任务
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
		//(a)检查
		select {
		case <-ctx.Done():
			_ = e.dst.CompleteBatch(context.Background(), batchID, "failed", totalCount)
			return ctx.Err()
		default:
		}

		//(b)拉取一页
		records, nextCursor, hasMore, err := e.src.Fetch(ctx, cursor, e.pageSize)
		if err != nil {
			return fmt.Errorf("Fetch is err: %w", err)
		}

		//(c)写一页 + 累加
		if len(records) > 0 {
			if err := e.dst.Save(ctx, records); err != nil {
				return fmt.Errorf("Save: %w", err)
			}
			totalCount += len(records)
		}

		//(d)判断是否为最后一页
		if hasMore == false {
			break
		}

		//(e)更新断点 + 进入下一轮
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
