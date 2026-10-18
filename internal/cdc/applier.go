package cdc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// errNoPrimaryKey 表示目标表没有主键。INSERT 据此降级为不幂等的裸 INSERT。
var errNoPrimaryKey = errors.New("目标表没有主键")

// TargetPoolFunc 按 connectionID 解析目标库连接池（惰性创建，内部缓存）。
type TargetPoolFunc func(ctx context.Context, connectionID int) (*pgxpool.Pool, error)

// Applier 把变更事件应用到对应连接的目标库的同名同结构表（镜像）。
// 多租户：每条事件按 connection_id 路由到各自目标库。
type Applier struct {
	poolFor TargetPoolFunc
	pools   map[int]*pgxpool.Pool
	mu      sync.Mutex

	// 下面三个缓存只在单条消费 goroutine 里读写（Serve 串行处理），故不加锁；
	// 只有 pools 跨 goroutine 访问才加锁。若将来并行化消费，这里需一并加锁。

	// "conn.schema.table" -> 主键列名
	pkCache map[string][]string

	// "conn.schema.table" -> 列名 -> 类型 OID
	colTypes map[string]map[string]uint32

	// "conn.schema.table" -> 是否已自动建表（幂等，避免每条事件都查/建）
	ensured map[string]bool
}

func NewApplier(poolFor TargetPoolFunc) *Applier {
	return &Applier{
		poolFor:  poolFor,
		pools:    map[int]*pgxpool.Pool{},
		pkCache:  map[string][]string{},
		colTypes: map[string]map[string]uint32{},
		ensured:  map[string]bool{},
	}
}

// pool 返回某连接的目标库连接池，带缓存。
//
// 注意：pools 是常驻缓存，连接被删除后对应池不会释放（学习项目可接受）。
func (a *Applier) pool(ctx context.Context, connectionID int) (*pgxpool.Pool, error) {
	a.mu.Lock()
	p := a.pools[connectionID]
	a.mu.Unlock()
	if p != nil {
		return p, nil
	}

	p, err := a.poolFor(ctx, connectionID)
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	a.pools[connectionID] = p
	a.mu.Unlock()
	return p, nil
}

func (a *Applier) Apply(ctx context.Context, ev Event) error {
	pool, err := a.pool(ctx, ev.ConnectionID)
	if err != nil {
		return err
	}

	// 缓存键带 connection_id：不同目标库的 public.documents 是两张不同的表
	key := fmt.Sprintf("%d.%s.%s", ev.ConnectionID, ev.Schema, ev.Table)
	table := pgx.Identifier{ev.Schema, ev.Table}.Sanitize()

	// 目标表可能还没建（快照没覆盖的新表）：用事件自带的 Relation 元数据自动建同构表
	if err := a.ensureTable(ctx, pool, key, ev.Schema, ev.Table, ev.Relation); err != nil {
		return err
	}

	switch ev.Operation {
	case "INSERT":
		pks, err := a.primaryKey(ctx, pool, key, ev.Schema, ev.Table)
		if err != nil {
			if errors.Is(err, errNoPrimaryKey) {
				// 无主键的表无法唯一去重，退回裸 INSERT（at-least-once 下可能重复，可接受）
				return a.insert(ctx, pool, table, key, ev.Schema, ev.Table, ev.Columns, nil)
			}
			return err
		}
		return a.insert(ctx, pool, table, key, ev.Schema, ev.Table, ev.Columns, pks)
	case "UPDATE":
		pks, err := a.primaryKey(ctx, pool, key, ev.Schema, ev.Table)
		if err != nil {
			return err
		}
		return a.update(ctx, pool, table, key, ev.Schema, ev.Table, ev.Columns, pks)
	case "DELETE":
		pks, err := a.primaryKey(ctx, pool, key, ev.Schema, ev.Table)
		if err != nil {
			return err
		}
		return a.delete(ctx, pool, table, key, ev.Schema, ev.Table, ev.Columns, pks)
	}

	slog.Warn("未知操作，跳过", "operation", ev.Operation)
	return nil
}

// ensureTable 目标表不存在时，用事件自带的 Relation 元数据自动建同构表（幂等）。
// rel 为 nil（老事件）时不做任何事，交由后续主键/列类型查询报错。
func (a *Applier) ensureTable(ctx context.Context, pool *pgxpool.Pool, key, schema, table string, rel *Relation) error {
	if rel == nil || len(rel.Columns) == 0 {
		return nil
	}
	if a.ensured[key] {
		return nil
	}
	if _, err := pool.Exec(ctx, buildCreateDDL(schema, table, rel)); err != nil {
		return fmt.Errorf("自动建表 %s.%s 失败: %w", schema, table, err)
	}
	a.ensured[key] = true
	slog.Warn("目标表不存在，已按源结构自动建表", "table", schema+"."+table)
	return nil
}

// buildCreateDDL 依据 Relation 元数据拼 CREATE TABLE IF NOT EXISTS（列名/表名 Sanitize 防注入）。
// 类型用 OID 映射的基名（如 int4、varchar、numeric），不保长度/精度修饰符——
// 自动建表只求能接住数据，精确类型由快照路径的 format_type 负责。
func buildCreateDDL(schema, table string, rel *Relation) string {
	parts := make([]string, 0, len(rel.Columns)+1)
	var pkCols []string
	for _, c := range rel.Columns {
		parts = append(parts, pgx.Identifier{c.Name}.Sanitize()+" "+oidTypeName(c.TypeOID))
		if c.IsKey {
			pkCols = append(pkCols, pgx.Identifier{c.Name}.Sanitize())
		}
	}
	if len(pkCols) > 0 {
		parts = append(parts, "PRIMARY KEY ("+strings.Join(pkCols, ", ")+")")
	}
	return "CREATE TABLE IF NOT EXISTS " + pgx.Identifier{schema, table}.Sanitize() +
		" (" + strings.Join(parts, ", ") + ")"
}

// oidTypeName 把列类型 OID 映射成 PG 类型名（建表用）；未知 OID 退回 text（最宽泛的文本类型）。
func oidTypeName(oid uint32) string {
	if typ, ok := typeMap.TypeForOID(oid); ok {
		return typ.Name
	}
	return "text"
}

func (a *Applier) insert(ctx context.Context, pool *pgxpool.Pool, table, key, schema, tableName string, cols []ColumnValue, pks []string) error {
	if len(cols) == 0 {
		return fmt.Errorf("INSERT 缺少列")
	}
	types, err := a.columnTypes(ctx, pool, key, schema, tableName)
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

	// at-least-once：reader 崩溃重放 / dispatcher 重发 / 死信补投都会让同一条 INSERT
	// 应用两次。有主键就加 ON CONFLICT DO NOTHING 幂等去重，第二次撞主键直接跳过。
	if len(pks) > 0 {
		pkNames := make([]string, 0, len(pks))
		for _, pk := range pks {
			pkNames = append(pkNames, pgx.Identifier{pk}.Sanitize())
		}
		sql += fmt.Sprintf(" ON CONFLICT (%s) DO NOTHING", strings.Join(pkNames, ", "))
	} else {
		slog.Warn("目标表无主键，INSERT 无法幂等", "table", tableName)
	}

	if _, err := pool.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("INSERT 失败: %w", err)
	}
	return nil
}

func (a *Applier) update(ctx context.Context, pool *pgxpool.Pool, table, key, schema, tableName string, cols []ColumnValue, pks []string) error {
	types, err := a.columnTypes(ctx, pool, key, schema, tableName)
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
		// 全主键表、或 UPDATE 只改了主键/TOAST 列：没有可更新的非主键列。
		// 降级为跳过（逻辑上等于无变化），避免报错进死信后反复重试同一条。
		slog.Warn("UPDATE 没有可更新的列，跳过", "table", tableName)
		return nil
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
	if _, err := pool.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("UPDATE 失败: %w", err)
	}
	return nil
}

func (a *Applier) delete(ctx context.Context, pool *pgxpool.Pool, table, key, schema, tableName string, cols []ColumnValue, pks []string) error {
	types, err := a.columnTypes(ctx, pool, key, schema, tableName)
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
			return fmt.Errorf("DELETE 缺少主键列 %s 的值(表需设主键或 REPLICA IDENTITY FULL)", pk)
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
	if _, err := pool.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("DELETE 失败: %w", err)
	}
	return nil
}

// coerceArgs 按目标列类型把 JSON 解码后的值转成正确的 Go 类型。
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
func (a *Applier) primaryKey(ctx context.Context, pool *pgxpool.Pool, key, schema, table string) ([]string, error) {
	if pks, ok := a.pkCache[key]; ok {
		return pks, nil
	}
	rows, err := pool.Query(ctx, `
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
		return nil, fmt.Errorf("表 %s.%s 没有主键: %w", schema, table, errNoPrimaryKey)
	}
	a.pkCache[key] = pks
	return pks, nil
}

// columnTypes 查目标表的列 -> 类型 OID，带缓存。
func (a *Applier) columnTypes(ctx context.Context, pool *pgxpool.Pool, key, schema, table string) (map[string]uint32, error) {
	if types, ok := a.colTypes[key]; ok {
		return types, nil
	}
	rows, err := pool.Query(ctx, `
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
