package reconciliation

import (
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/bobdfy/syncguard/internal/model"
	"github.com/bobdfy/syncguard/internal/repository"
	"github.com/bobdfy/syncguard/internal/source"
)

/*
Reconciler 对账引擎。

对账 = 拿源端当前数据，跟目标端（synced_records）逐条对比，找出不一致。

流程：
 1. 查 connections 表 → 拿到 source 配置
 2. 通过工厂创建 Source（复用 NewSource，和 Worker 用同一个工厂）
 3. 分页读取源端全部数据 → 放进 map
 4. 分页读取 synced_records → 放进 map
 5. 对比两个 map，分四类差异

四类差异：
  missing_in_target  — 源端有，目标端缺失（同步漏了）
  extra_in_target    — 目标端有，源端没有（源端删了这条数据）
  version_mismatch   — 版本号不一样（目标端是旧版本）
  content_mismatch   — 版本号一样但内容 Hash 不同（数据被改了）

使用方式：
  reconciler := NewReconciler(db, connectionStore)
  diffs, err := reconciler.Reconcile(ctx, connectionID)
*/

type Reconciler struct {
	db              *repository.DB
	connectionStore *repository.ConnectionStore
}

// NewReconciler 创建对账引擎。
func NewReconciler(db *repository.DB, connectionStore *repository.ConnectionStore) *Reconciler {
	return &Reconciler{db: db, connectionStore: connectionStore}
}

/*
Reconcile 执行对账，返回差异列表。

参数：

	ctx          — 上下文（超时控制）
	connectionID — 要检查的数据源连接 ID

返回：

	diffs — 差异列表，没有差异时为空切片
	err   — 查询/网络错误
*/
func (r *Reconciler) Reconcile(ctx context.Context, connectionID int, userID int) ([]model.Diff, error) {

	// 第 1 步：创建 Source，拿到源端数据访问能力
	// 复用 Worker 那条工厂链路：
	//   connectionID → 查 connections 表 → switch source_type → 创建具体 Source
	//
	src, err := source.NewSource(ctx, r.db, r.connectionStore, connectionID)
	if err != nil {
		return nil, fmt.Errorf("创建数据源失败: %w", err)
	}

	// 第 2 步：分页读取源端全部数据 → 放进 map
	// cursor 从空字符串开始（Source 会把 "" 当成第 1 页），
	// 循环直到 hasMore == false。
	sourceMap := make(map[string]model.Record) // key = Record.ID
	cursor := ""

	for {
		records, nextcursor, hasmore, err := src.Fetch(ctx, cursor, 100)
		if err != nil {
			return nil, fmt.Errorf("同步数据时发生错误: %v", err)
		}
		for _, rec := range records {
			sourceMap[rec.ID] = rec
		}
		cursor = nextcursor
		if hasmore == false {
			break
		}
	}

	// 第 3 步：分页读取目标端（synced_records）→ 放进 map
	// 因为 synced_records 是所有数据源混在一起的，这里简化处理：
	// 只对比 ID 以 "issue_" 开头的记录（GitHub Issue）。
	// 后续加了其他数据源再扩展。
	syncedStore := repository.NewSyncedStore(r.db)
	targetMap := make(map[string]model.Record)
	offset := 0

	for {
		records, err := syncedStore.ListRecordsByUser(ctx, userID, 100, offset)
		if err != nil {
			return nil, fmt.Errorf("查询目标端失败: %w", err)
		}
		if len(records) == 0 {
			break
		}
		for _, rec := range records {
			targetMap[rec.ID] = rec
		}
		offset += len(records)
	}

	// 第 4 步：对比两个 map，分类差异
	var diffs []model.Diff

	// 4a. 源端有 → 检查目标端
	for id, srcRec := range sourceMap {
		tgtRec, exists := targetMap[id]

		if !exists {
			// 目标端缺失
			diffs = append(diffs, model.Diff{
				SourceID:      id,
				DiffType:      "missing_in_target",
				SourceVersion: srcRec.Version,
				SourceHash:    hash(srcRec.Data),
			})
			continue
		}

		// 版本比较
		if srcRec.Version != tgtRec.Version {
			diffs = append(diffs, model.Diff{
				SourceID:      id,
				DiffType:      "version_mismatch",
				SourceVersion: srcRec.Version,
				TargetVersion: tgtRec.Version,
				SourceHash:    hash(srcRec.Data),
				TargetHash:    hash(tgtRec.Data),
			})
			continue
		}

		// 版本号一样但内容 Hash 不同 → 内容被篡改
		srcHash := hash(srcRec.Data)
		tgtHash := hash(tgtRec.Data)
		if srcHash != tgtHash {
			diffs = append(diffs, model.Diff{
				SourceID:      id,
				DiffType:      "content_mismatch",
				SourceVersion: srcRec.Version,
				TargetVersion: tgtRec.Version,
				SourceHash:    srcHash,
				TargetHash:    tgtHash,
			})
		}
	}

	// 4a 已经把源端有的全比过了，这里只找目标端多出来的
	for id, tgtRec := range targetMap {
		if _, exists := sourceMap[id]; !exists {
			diffs = append(diffs, model.Diff{
				SourceID:      id,
				DiffType:      "extra_in_target",
				TargetVersion: tgtRec.Version,
				TargetHash:    hash(tgtRec.Data),
			})
		}
	}
	return diffs, nil
}

// hash 计算数据的 SHA256 Hash，返回十六进制字符串。
// 对账时用 Hash 比较内容，避免把整个 JSONB 逐字段比。
func hash(data []byte) string {
	h := sha256.Sum256(data)
	return fmt.Sprintf("%x", h)
}
