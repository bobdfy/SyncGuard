package reconciliation

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/bobdfy/syncguard/internal/engine"
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
func (r *Reconciler) Reconcile(ctx context.Context, connectionID int, userID int, syncContent string, targetConnectionID *int) ([]model.Diff, error) {

	// 第 1 步：创建 Source，拿到源端数据访问能力
	// 复用 Worker 那条工厂链路：
	//   connectionID → 查 connections 表 → switch source_type → 创建具体 Source
	//
	src, err := source.NewSource(ctx, r.db, r.connectionStore, connectionID, syncContent)
	if err != nil {
		return nil, fmt.Errorf("创建数据源失败: %w", err)
	}
	defer func() { _ = src.Close() }()

	// 镜像轨道：源是数据库表且配了数据库目标 → 直接对比两张表（列对列）。
	if tableSrc, ok := src.(engine.TableSource); ok && targetConnectionID != nil && *targetConnectionID != 0 {
		return r.reconcileTable(ctx, tableSrc, *targetConnectionID, syncContent)
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
	// 按 user_id + connection_id 过滤，只对比当前数据源的数据，避免多源混账。
	syncedStore := repository.NewSyncedStore(r.db)
	targetMap := make(map[string]model.Record)
	offset := 0

	for {
		records, err := syncedStore.ListRecordsByUserAndConnection(ctx, userID, connectionID, 100, offset)
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

	// 第 4 步：对比两个 map，分类差异（纯函数，独立可测）
	diffs := compareMaps(sourceMap, targetMap)
	return diffs, nil
}

// reconcileTable 镜像轨道对账：对比源表与目标表（两张外部 PG 表）。
//
// 目标表读取器复用 source.NewSource（目标连接也是 PG，返回的 *postgres.Source 天然满足 TableSource）。
// 逐行按主键 + 整行 Hash 对比，产出四类差异；镜像无版本号，Version 恒 0，故只可能出现
// missing_in_target / extra_in_target / content_mismatch 三类。
func (r *Reconciler) reconcileTable(ctx context.Context, tableSrc engine.TableSource, targetConnectionID int, syncContent string) ([]model.Diff, error) {
	dst, err := source.NewSource(ctx, r.db, r.connectionStore, targetConnectionID, syncContent)
	if err != nil {
		return nil, fmt.Errorf("创建目标表读取器失败: %w", err)
	}
	defer func() { _ = dst.Close() }()

	tableDst, ok := dst.(engine.TableSource)
	if !ok {
		return nil, fmt.Errorf("目标端不是数据库表: %w", engine.ErrNonRetryable)
	}

	sourceMap, err := readAllRows(ctx, tableSrc)
	if err != nil {
		return nil, fmt.Errorf("读取源表失败: %w", err)
	}
	targetMap, err := readAllRows(ctx, tableDst)
	if err != nil {
		return nil, fmt.Errorf("读取目标表失败: %w", err)
	}
	return compareMaps(sourceMap, targetMap), nil
}

// readAllRows 把一张表读成 map[主键]model.Record，Data 为该行的规范 JSON（供 hash 比较）。
// 镜像对账复用信封对账的 compareMaps：Version 恒 0，差异只可能是 缺失/多余/内容不一致。
func readAllRows(ctx context.Context, ts engine.TableSource) (map[string]model.Record, error) {
	schema, err := ts.TableSchema(ctx)
	if err != nil {
		return nil, fmt.Errorf("读取表结构失败: %w", err)
	}
	if len(schema.PrimaryKey) != 1 {
		return nil, fmt.Errorf("镜像对账仅支持单列主键: %w", engine.ErrNonRetryable)
	}
	pkName := schema.PrimaryKey[0]
	pkIdx := -1
	for i, c := range schema.Columns {
		if c.Name == pkName {
			pkIdx = i
			break
		}
	}
	if pkIdx < 0 {
		return nil, fmt.Errorf("主键列 %q 不在表结构中: %w", pkName, engine.ErrNonRetryable)
	}

	result := make(map[string]model.Record)
	cursor := ""
	for {
		rows, next, hasMore, err := ts.FetchRows(ctx, cursor, 100)
		if err != nil {
			return nil, fmt.Errorf("读取表数据失败: %w", err)
		}
		for _, row := range rows {
			if pkIdx >= len(row.Values) {
				continue // 防御：行列数对不上
			}
			pk := fmt.Sprintf("%v", row.Values[pkIdx])
			data, _ := json.Marshal(row.Values)
			result[pk] = model.Record{ID: pk, Data: data}
		}
		cursor = next
		if !hasMore {
			break
		}
	}
	return result, nil
}

// compareMaps 对比源端与目标端两个 map，产出四类差异。
//
// 纯函数：不碰 DB / 网络，输入两个 map，输出差异列表，便于单元测试。
//
// 四类差异：
//
//	missing_in_target  — 源端有，目标端缺失（同步漏了）
//	extra_in_target    — 目标端有，源端没有（源端删了这条数据）
//	version_mismatch   — 版本号不一样（目标端是旧版本）
//	content_mismatch   — 版本号一样但内容 Hash 不同（数据被改了）
func compareMaps(sourceMap, targetMap map[string]model.Record) []model.Diff {
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

	// 4b. 源端有的已经全比过，这里只找目标端多出来的
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
	return diffs
}

// hash 计算数据的 SHA256 Hash，返回十六进制字符串。
// 对账时用 Hash 比较内容，避免把整个 JSONB 逐字段比。
func hash(data []byte) string {
	h := sha256.Sum256(data)
	return fmt.Sprintf("%x", h)
}
