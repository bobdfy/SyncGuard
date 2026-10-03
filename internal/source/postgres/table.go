package postgres

import (
	"context"
	"fmt"

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
// 副作用：把解析出的真实主键缓存到 s.pk，供后续 FetchRows 排序用；
// 镜像轨道会覆盖 sync_content 里指定的 pk（以真实主键为准，保证 ON CONFLICT 唯一）。
func (s *Source) TableSchema(ctx context.Context) (model.TableSchema, error) {
	// 1. 列名 + 类型 + 非空，按列序
	rows, err := s.pool.Query(ctx, `
		SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relname = $1 AND a.attnum > 0 AND NOT a.attisdropped
		  AND n.nspname = ANY (current_schemas(false))
		ORDER BY a.attnum`, s.table)
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
		  AND tc.table_schema = ANY (current_schemas(false))
		ORDER BY kcu.ordinal_position`, s.table)
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
	if len(pkCols) != 1 {
		return model.TableSchema{}, fmt.Errorf("镜像轨道仅支持单列主键（表 %q 主键列数=%d）: %w", s.table, len(pkCols), engine.ErrNonRetryable)
	}

	// 3. 缓存真实主键供 FetchRows 用
	s.pk = pkCols[0]

	return model.TableSchema{TableName: s.table, Columns: cols, PrimaryKey: pkCols}, nil
}

// FetchRows 按主键 keyset 分页取原生行。
//
// 与信封轨道的 Fetch 保持同一套分页语义：游标用主键 ::text 的稳定全序比较，
// 兼容任意标量主键类型。数据本身用 pgx 的 rows.Values() 取原生 Go 值，
// 不经 row_to_json 字符串化，交给目标端列对列写入以保真。
func (s *Source) FetchRows(ctx context.Context, cursor string, limit int) ([]model.RawRow, string, bool, error) {
	select {
	case <-ctx.Done():
		return nil, "", false, ctx.Err()
	default:
	}

	if limit <= 0 {
		return nil, "", false, fmt.Errorf("limit必须大于0")
	}

	query := fmt.Sprintf(`SELECT * FROM %s ORDER BY %s::text LIMIT $1`, s.table, s.pk)
	args := []any{limit}
	if cursor != "" {
		query = fmt.Sprintf(`SELECT * FROM %s WHERE %s::text > $1 ORDER BY %s::text LIMIT $2`, s.table, s.pk, s.pk)
		args = []any{cursor, limit}
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, "", false, fmt.Errorf("查询源表失败: %w", err)
	}
	defer rows.Close()

	// 主键列在结果集中的下标，用于取游标值。
	fds := rows.FieldDescriptions()
	pkIdx := -1
	for i, fd := range fds {
		if fd.Name == s.pk {
			pkIdx = i
			break
		}
	}

	result := make([]model.RawRow, 0, limit)
	var lastKey string
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, "", false, fmt.Errorf("读取行值失败: %w", err)
		}
		if pkIdx >= 0 && pkIdx < len(values) {
			lastKey = fmt.Sprintf("%v", values[pkIdx])
		}
		result = append(result, model.RawRow{Values: values})
	}
	if err := rows.Err(); err != nil {
		return nil, "", false, fmt.Errorf("遍历行失败: %w", err)
	}

	hasMore := len(result) == limit
	nextCursor := ""
	if hasMore {
		nextCursor = lastKey
	}
	return result, nextCursor, hasMore, nil
}
