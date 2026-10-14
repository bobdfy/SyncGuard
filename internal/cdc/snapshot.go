package cdc

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	dstpostgres "github.com/bobdfy/syncguard/internal/destination/postgres"
	"github.com/bobdfy/syncguard/internal/engine"
	srcpostgres "github.com/bobdfy/syncguard/internal/source/postgres"
)

// Snapshot 一次性存量快照：钉切点（建槽）→ 记录断点 → 搬存量到目标库。
//
// 之后正常启动 cdcreader 会从断点（切点 C）开始接增量，存量+增量无缝衔接：
//   - C 之前提交的变更：由本快照搬过去；
//   - C 之后（含复制期间）的变更：由增量流从 C 重放，重叠靠目标端 ON CONFLICT 幂等兜掉。
func Snapshot(ctx context.Context, dsn, targetDSN, slotName, table string, store *EventStore) error {
	if store == nil {
		return fmt.Errorf("EventStore 不能为空")
	}

	// 1. 钉切点：建复制连接 + 确保槽存在；新建槽的当前 WAL 位点即切点 C
	conn, cutoverLSN, created, err := ensureSlot(ctx, dsn, slotName)
	if err != nil {
		return fmt.Errorf("钉切点失败: %w", err)
	}
	defer func() {
		cctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = conn.Close(cctx)
	}()

	if created {
		// 首次接入：把切点 C 落进 cdc_checkpoints，后续 reader 从 C 恢复
		if err := store.SaveCheckpoint(ctx, cutoverLSN.String()); err != nil {
			return err
		}
		slog.Info("存量快照：已钉切点", "slot", slotName, "cutover_lsn", cutoverLSN.String())
	} else {
		// 槽已存在：断点已写过（或可由 slot restart_lsn 兜底），不再动断点，只做幂等搬运（重跑安全）
		slog.Warn("复制槽已存在，跳过钉切点，仅搬运存量", "slot", slotName)
	}

	// 2. 搬存量：复用引擎镜像轨道（TableSchema → 建同构表 → keyset 分页 → 幂等 upsert）
	src, err := srcpostgres.NewSource(ctx, dsn, table)
	if err != nil {
		return fmt.Errorf("创建源失败: %w", err)
	}
	defer src.Close()

	dst, err := dstpostgres.NewTableDestination(ctx, targetDSN, table)
	if err != nil {
		return fmt.Errorf("创建目标失败: %w", err)
	}
	defer dst.Close()

	if err := engine.RunTable(ctx, src, dst, noopProgress{}, "cdc-snapshot", 200); err != nil {
		return fmt.Errorf("存量搬运失败: %w", err)
	}

	slog.Info("存量快照完成", "slot", slotName, "table", table)
	return nil
}

// noopProgress 快照专用：不走 sync_jobs 断点，纯一次性搬运。
// RunTable 的断点/批次语义是为可重试任务设计的，快照是一次性的，这里全部空实现。
type noopProgress struct{}

var _ engine.Progress = noopProgress{}

func (noopProgress) GetCheckpoint(context.Context, string) (string, error)   { return "", nil }
func (noopProgress) UpdateCheckpoint(context.Context, string, string) error  { return nil }
func (noopProgress) CreateBatch(context.Context) (int64, error)              { return 0, nil }
func (noopProgress) CompleteBatch(context.Context, int64, string, int) error { return nil }
