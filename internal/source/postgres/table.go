package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bobdfy/syncguard/internal/engine"
	"github.com/bobdfy/syncguard/internal/model"
)

// 编译期检查：*Source 同时实现 engine.Source（信封）和 engine.TableSource（镜像）。
var _ engine.TableSource = (*Source)(nil)

// TableSchema 读源表结构（列名 + 原生类型 + 主键）。
//
// 列信息走 pg_attribute + format_type：拿到精确到修饰符的原生类型
// （如 character varying(255)、numeric(10,2)、timestamp with time zone），
// 目标端据此建同构表才谈得上「类型保真」。主键走 information_schema 标准视图。
//
// 副作用：把解析出的真实主键列缓存到 s.pkCols，供后续 FetchRows 排序用；
// 镜像轨道会覆盖 sync_content 里指定的 pk（以真实主键为准，保证 ON CONFLICT 唯一）。
func (s *Source) TableSchema(ctx context.Context) (model.TableSchema, error) {
	// 1. 列名 + 类型 + 非空，按列序
	rows, err := s.pool.Query(ctx, `
		SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relname = $1 AND a.attnum > 0 AND NOT a.attisdropped
		  AND n.nspname = $2
		ORDER BY a.attnum`, s.table, s.schema)
	if err != nil {
		return model.TableSchema{}, fmt.Errorf("读取列结构失败: %w", err)
	}
	defer rows.Close()

	cols := make([]model.Column, 0, 8)
	for rows.Next() {
		var c model.Column
		if err := rows.Scan(&c.Name, &c.DataType, &c.Nullable); err != nil {
			return model.TableSchema{}, fmt.Errorf("扫描列结构失败: %w", err)
		}
		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		return model.TableSchema{}, fmt.Errorf("遍历列结构失败: %w", err)
	}
	if len(cols) == 0 {
		return model.TableSchema{}, fmt.Errorf("表 %q 不存在或无可见列: %w", s.table, engine.ErrNonRetryable)
	}

	// 2. 主键列（按主键内顺序）
	pkRows, err := s.pool.Query(ctx, `
		SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		  ON kcu.constraint_name = tc.constraint_name
		 AND kcu.constraint_schema = tc.constraint_schema
		 AND kcu.table_name = tc.table_name
		WHERE tc.constraint_type = 'PRIMARY KEY'
		  AND tc.table_name = $1
		  AND tc.table_schema = $2
		ORDER BY kcu.ordinal_position`, s.table, s.schema)
	if err != nil {
		return model.TableSchema{}, fmt.Errorf("读取主键失败: %w", err)
	}
	defer pkRows.Close()

	var pkCols []string
	for pkRows.Next() {
		var name string
		if err := pkRows.Scan(&name); err != nil {
			return model.TableSchema{}, fmt.Errorf("扫描主键失败: %w", err)
		}
		pkCols = append(pkCols, name)
	}
	if err := pkRows.Err(); err != nil {
		return model.TableSchema{}, fmt.Errorf("遍历主键失败: %w", err)
	}
	if len(pkCols) == 0 {
		return model.TableSchema{}, fmt.Errorf("镜像轨道要求表有主键（表 %q 无主键，无法 keyset 分页）: %w", s.table, engine.ErrNonRetryable)
	}

	// 3. 缓存真实主键列（有序）供 FetchRows 做复合 keyset 分页
	s.pkCols = pkCols

	return model.TableSchema{TableName: s.table, Columns: cols, PrimaryKey: pkCols}, nil
}

// FetchRows 按主键 keyset 分页取原生行。
//
// 与信封轨道的 Fetch 保持同一套分页语义：游标是主键各列 ::text 的 JSON 数组
// （单列主键是 ["v"]，复合主键是 ["v1","v2"]），ORDER BY / WHERE 用主键列的自然类型
// 做元组比较，兼容任意标量主键类型（含复合主键）。数据本身用 pgx 的 rows.Values()
// 取原生 Go 值，不经 row_to_json 字符串化，交给目标端列对列写入以保真。
func (s *Source) FetchRows(ctx context.Context, cursor string, limit int) ([]model.RawRow, string, bool, error) {
	select {
	case <-ctx.Done():
		return nil, "", false, ctx.Err()
	default:
	}

	if limit <= 0 {
		return nil, "", false, fmt.Errorf("limit必须大于0")
	}

	pkCols := s.pkCols
	if len(pkCols) == 0 {
		return nil, "", false, fmt.Errorf("表 %q 主键未知（需先调 TableSchema）: %w", s.table, engine.ErrNonRetryable)
	}

	// ORDER BY / WHERE 用主键列的自然类型：语义正确（数值按数值序、UUID 按字节序），
	// 且与元组比较 (pk1,pk2) > ($1,$2) 严格一致，不会跳行/重行。
	orderExpr := strings.Join(pkCols, ", ")

	// 追加每个主键列的 ::text 作游标分量：游标由 PG 自己格式化，保证与下一次
	// (pk1,pk2) > ($1,$2) 的入参严格同形（之前手工 %v 格式化 uuid 会输出 [123 45 ...] 跳行）。
	cursorCols := make([]string, len(pkCols))
	for i, c := range pkCols {
		// 加别名避免与 SELECT * 展开的同名列冲突（否则 ORDER BY pk 会歧义）
		cursorCols[i] = fmt.Sprintf("%s::text AS _syncguard_cursor_%d", c, i)
	}
	cursorSelect := strings.Join(cursorCols, ", ")

	table := s.qualifiedName()
	var query string
	var args []any
	if cursor == "" {
		query = fmt.Sprintf("SELECT *, %s FROM %s ORDER BY %s LIMIT $1", cursorSelect, table, orderExpr)
		args = []any{limit}
	} else {
		// 游标是 JSON 数组的主键文本分量（与 nextCursor 同形）
		var vals []string
		if err := json.Unmarshal([]byte(cursor), &vals); err != nil {
			return nil, "", false, fmt.Errorf("解析游标失败: %w", err)
		}
		if len(vals) != len(pkCols) {
			return nil, "", false, fmt.Errorf("游标主键列数不匹配（%d vs %d）", len(vals), len(pkCols))
		}
		placeholders := make([]string, len(vals))
		for i := range vals {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		}
		query = fmt.Sprintf("SELECT *, %s FROM %s WHERE (%s) > (%s) ORDER BY %s LIMIT $%d",
			cursorSelect, table, orderExpr, strings.Join(placeholders, ", "), orderExpr, len(vals)+1)
		args = make([]any, 0, len(vals)+1)
		for _, v := range vals {
			args = append(args, v)
		}
		args = append(args, limit)
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, "", false, fmt.Errorf("查询源表失败: %w", err)
	}
	defer rows.Close()

	n := len(pkCols)
	result := make([]model.RawRow, 0, limit)
	var cursorVals []string
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, "", false, fmt.Errorf("读取行值失败: %w", err)
		}
		if len(values) < n {
			return nil, "", false, fmt.Errorf("行值列数(%d)少于游标分量数(%d)", len(values), n)
		}
		// 末尾 n 列是 pk::text 游标分量，单独取出；RawRow 只保留原生列，避免污染目标写入。
		cursorVals = make([]string, n)
		for i := 0; i < n; i++ {
			// pk::text 恒扫成 string，ok 断言仅作防御。
			if s, ok := values[len(values)-n+i].(string); ok {
				cursorVals[i] = s
			}
		}
		result = append(result, model.RawRow{Values: values[:len(values)-n]})
	}
	if err := rows.Err(); err != nil {
		return nil, "", false, fmt.Errorf("遍历行失败: %w", err)
	}

	hasMore := len(result) == limit
	// 即使最后一页（hasMore=false）也返回末行主键作游标：让引擎把最后一页一并 checkpoint
	nextCursor := ""
	if len(result) > 0 {
		b, err := json.Marshal(cursorVals)
		if err != nil {
			return nil, "", false, fmt.Errorf("序列化游标失败: %w", err)
		}
		nextCursor = string(b)
	}
	return result, nextCursor, hasMore, nil
}
