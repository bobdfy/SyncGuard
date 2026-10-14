package cdc

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Applier 把变更事件应用到目标库的同名同结构表（镜像）。
type Applier struct {
	pool     *pgxpool.Pool
	pkCache  map[string][]string          // "schema.table" -> 主键列名（有序）
	colTypes map[string]map[string]uint32 // "schema.table" -> 列名 -> 类型 OID
}

func NewApplier(pool *pgxpool.Pool) *Applier {
	return &Applier{
		pool:     pool,
		pkCache:  map[string][]string{},
		colTypes: map[string]map[string]uint32{},
	}
}

func (a *Applier) Apply(ctx context.Context, ev Event) error {
	key := ev.Schema + "." + ev.Table
	table := pgx.Identifier{ev.Schema, ev.Table}.Sanitize()

	switch ev.Operation {
	case "INSERT":
		return a.insert(ctx, table, key, ev.Schema, ev.Table, ev.Columns)
	case "UPDATE":
		pks, err := a.primaryKey(ctx, key, ev.Schema, ev.Table)
		if err != nil {
			return err
		}
		return a.update(ctx, table, key, ev.Schema, ev.Table, ev.Columns, pks)
	case "DELETE":
		pks, err := a.primaryKey(ctx, key, ev.Schema, ev.Table)
		if err != nil {
			return err
		}
		return a.delete(ctx, table, key, ev.Schema, ev.Table, ev.Columns, pks)
	}

	slog.Warn("未知操作，跳过", "operation", ev.Operation)
	return nil
}

func (a *Applier) insert(ctx context.Context, table, key, schema, tableName string, cols []ColumnValue) error {
	if len(cols) == 0 {
		return fmt.Errorf("INSERT 缺少列")
	}
	types, err := a.columnTypes(ctx, key, schema, tableName)
	if err != nil {
		return err
	}
	args, err := coerceArgs(types, cols)
	if err != nil {
		return err
	}

	names := make([]string, 0, len(cols))
	placeholders := make([]string, 0, len(cols))
	for i, c := range cols {
		names = append(names, pgx.Identifier{c.Name}.Sanitize())
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		table, strings.Join(names, ", "), strings.Join(placeholders, ", "))
	if _, err := a.pool.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("INSERT 失败: %w", err)
	}
	return nil
}

func (a *Applier) update(ctx context.Context, table, key, schema, tableName string, cols []ColumnValue, pks []string) error {
	types, err := a.columnTypes(ctx, key, schema, tableName)
	if err != nil {
		return err
	}

	valMap := map[string]ColumnValue{}
	for _, c := range cols {
		valMap[c.Name] = c
	}
	pkSet := map[string]bool{}
	for _, pk := range pks {
		pkSet[pk] = true
	}

	// SET：非主键列
	var setCols []ColumnValue
	for _, c := range cols {
		if pkSet[c.Name] {
			continue
		}
		setCols = append(setCols, c)
	}
	if len(setCols) == 0 {
		return fmt.Errorf("UPDATE 没有可更新的列")
	}
	setArgs, err := coerceArgs(types, setCols)
	if err != nil {
		return err
	}

	// WHERE：主键列
	var whereCols []ColumnValue
	for _, pk := range pks {
		c, ok := valMap[pk]
		if !ok {
			return fmt.Errorf("UPDATE 缺少主键列 %s 的值", pk)
		}
		whereCols = append(whereCols, c)
	}
	whereArgs, err := coerceArgs(types, whereCols)
	if err != nil {
		return err
	}

	sets := make([]string, 0, len(setCols))
	for i, c := range setCols {
		sets = append(sets, fmt.Sprintf("%s = $%d", pgx.Identifier{c.Name}.Sanitize(), i+1))
	}
	wheres := make([]string, 0, len(whereCols))
	for i, c := range whereCols {
		wheres = append(wheres, fmt.Sprintf("%s = $%d", pgx.Identifier{c.Name}.Sanitize(), len(setCols)+i+1))
	}
	args := append(setArgs, whereArgs...)

	sql := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		table, strings.Join(sets, ", "), strings.Join(wheres, " AND "))
	if _, err := a.pool.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("UPDATE 失败: %w", err)
	}
	return nil
}

func (a *Applier) delete(ctx context.Context, table, key, schema, tableName string, cols []ColumnValue, pks []string) error {
	types, err := a.columnTypes(ctx, key, schema, tableName)
	if err != nil {
		return err
	}

	valMap := map[string]ColumnValue{}
	for _, c := range cols {
		valMap[c.Name] = c
	}

	var whereCols []ColumnValue
	for _, pk := range pks {
		c, ok := valMap[pk]
		if !ok {
			return fmt.Errorf("DELETE 缺少主键列 %s 的值（表需设主键或 REPLICA IDENTITY FULL）", pk)
		}
		whereCols = append(whereCols, c)
	}
	args, err := coerceArgs(types, whereCols)
	if err != nil {
		return err
	}

	wheres := make([]string, 0, len(whereCols))
	for i, c := range whereCols {
		wheres = append(wheres, fmt.Sprintf("%s = $%d", pgx.Identifier{c.Name}.Sanitize(), i+1))
	}

	sql := fmt.Sprintf("DELETE FROM %s WHERE %s", table, strings.Join(wheres, " AND "))
	if _, err := a.pool.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("DELETE 失败: %w", err)
	}
	return nil
}

// coerceArgs 按目标列类型把 JSON 解码后的值转成正确的 Go 类型。
// 关键点：事件经 RabbitMQ 是 JSON 序列化，int 变 float64、time.Time 变 string，
// 若原样塞给 pgx 会报「类型不匹配」，这里按列 OID 重新解码回正确类型。
func coerceArgs(types map[string]uint32, cols []ColumnValue) ([]any, error) {
	args := make([]any, 0, len(cols))
	for _, c := range cols {
		if c.Null {
			args = append(args, nil)
			continue
		}
		oid, ok := types[c.Name]
		if !ok {
			args = append(args, c.Value) // 类型未知，原样传
			continue
		}
		v, err := coerceValue(oid, c.Value)
		if err != nil {
			return nil, fmt.Errorf("列 %s 类型转换失败: %w", c.Name, err)
		}
		args = append(args, v)
	}
	return args, nil
}

func coerceValue(oid uint32, v any) (any, error) {
	if v == nil {
		return nil, nil
	}

	// 时间类型：JSON 里是 RFC3339（带 T），pgtype 文本解码只认空格分隔，直接 parse 成 time.Time
	switch oid {
	case pgtype.TimestamptzOID, pgtype.TimestampOID, pgtype.DateOID:
		if s, ok := v.(string); ok {
			t, err := parseTime(s)
			if err != nil {
				return nil, err
			}
			return t, nil
		}
	}

	var text []byte
	switch x := v.(type) {
	case string:
		text = []byte(x)
	case float64:
		text = []byte(strconv.FormatFloat(x, 'f', -1, 64))
	case bool:
		text = []byte(strconv.FormatBool(x))
	case json.RawMessage:
		text = []byte(x)
	case map[string]any, []any:
		b, err := json.Marshal(x)
		if err != nil {
			return nil, err
		}
		text = b
	default:
		text = fmt.Append(nil, v)
	}

	typ, ok := typeMap.TypeForOID(oid)
	if !ok {
		return string(text), nil // 未知 OID，退回字符串
	}
	return typ.Codec.DecodeValue(typeMap, oid, pgtype.TextFormatCode, text)
}

func parseTime(s string) (time.Time, error) {
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999Z07",
		"2006-01-02 15:04:05Z07",
		"2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("无法解析时间 %q", s)
}

// primaryKey 查目标表的主键列（有序），带缓存。
func (a *Applier) primaryKey(ctx context.Context, key, schema, table string) ([]string, error) {
	if pks, ok := a.pkCache[key]; ok {
		return pks, nil
	}
	rows, err := a.pool.Query(ctx, `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_class c ON c.oid = i.indrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
		WHERE i.indisprimary AND n.nspname = $1 AND c.relname = $2
		ORDER BY array_position(i.indkey, a.attnum)`, schema, table)
	if err != nil {
		return nil, fmt.Errorf("查询主键失败: %w", err)
	}
	defer rows.Close()

	var pks []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("扫描主键失败: %w", err)
		}
		pks = append(pks, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历主键失败: %w", err)
	}
	if len(pks) == 0 {
		return nil, fmt.Errorf("表 %s.%s 没有主键，无法处理 UPDATE/DELETE", schema, table)
	}
	a.pkCache[key] = pks
	return pks, nil
}

// columnTypes 查目标表的列 -> 类型 OID，带缓存。
func (a *Applier) columnTypes(ctx context.Context, key, schema, table string) (map[string]uint32, error) {
	if types, ok := a.colTypes[key]; ok {
		return types, nil
	}
	rows, err := a.pool.Query(ctx, `
		SELECT a.attname, a.atttypid
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_attribute a ON a.attrelid = c.oid
		WHERE a.attnum > 0 AND NOT a.attisdropped
		  AND n.nspname = $1 AND c.relname = $2`, schema, table)
	if err != nil {
		return nil, fmt.Errorf("查询列类型失败: %w", err)
	}
	defer rows.Close()

	types := map[string]uint32{}
	for rows.Next() {
		var name string
		var oid uint32
		if err := rows.Scan(&name, &oid); err != nil {
			return nil, fmt.Errorf("扫描列类型失败: %w", err)
		}
		types[name] = oid
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历列类型失败: %w", err)
	}
	a.colTypes[key] = types
	return types, nil
}
