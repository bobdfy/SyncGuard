package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/bobdfy/syncguard/internal/engine"
	"github.com/bobdfy/syncguard/internal/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Source struct {
	dbURL  string
	schema string
	table  string
	pk     string
	// pkCols 是 TableSchema 解析出的真实主键列（有序）。镜像轨道（FetchRows）用它做
	// 复合 keyset 分页；信封轨道（Fetch）仍用 sync_content 指定的单列 pk。
	pkCols []string
	pool   *pgxpool.Pool
}

var rule = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// splitQualified 把 "schema.table" 拆成 schema + table；无 "." 前缀时 schema 默认 public。
func splitQualified(name string) (schema, table string, err error) {
	schema, table = "public", name
	if i := strings.LastIndex(name, "."); i >= 0 {
		schema, table = name[:i], name[i+1:]
	}
	if !rule.MatchString(schema) || !rule.MatchString(table) {
		return "", "", fmt.Errorf("非法表名 %q（只允许字母/数字/下划线，可带 schema 前缀）", name)
	}
	return schema, table, nil
}

// parseContent 解析 sync_content："[schema.]table[:pk]"。schema 缺省 public、pk 缺省 id。
func parseContent(syncContent string) (schema, table, pk string, err error) {
	content := strings.SplitN(syncContent, ":", 2)
	table = content[0]
	pk = "id"
	if len(content) == 2 {
		pk = content[1]
	}

	schema, table, err = splitQualified(table)
	if err != nil {
		return "", "", "", fmt.Errorf("%w: %v", engine.ErrNonRetryable, err)
	}
	if !rule.MatchString(pk) {
		return "", "", "", fmt.Errorf("非法主键: %v, err: %w", pk, engine.ErrNonRetryable)
	}
	return schema, table, pk, nil
}

func NewSource(ctx context.Context, dbURL string, syncContent string) (*Source, error) {
	schema, table, pk, err := parseContent(syncContent)
	if err != nil {
		return nil, fmt.Errorf("未成功通过检验: %v", err)
	}

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return nil, fmt.Errorf("创建连接池失败: %w", err)
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pool.Ping: %w", err)
	}

	return &Source{dbURL: dbURL, schema: schema, table: table, pk: pk, pool: pool}, nil
}

// Close 释放连接池。调用方（Worker/Reconciler）用完 Source 后必须 defer 关闭，
// 否则每次同步/对账都会泄漏一个连接池（每个池持有自己的连接，反复跑会连接耗尽）。
func (s *Source) Close() error {
	if s.pool != nil {
		s.pool.Close()
	}
	return nil
}

// qualifiedName 返回 "schema.table"。两者都经白名单校验，可安全拼进 SQL。
func (s *Source) qualifiedName() string {
	return s.schema + "." + s.table
}

func (s *Source) Fetch(ctx context.Context, cursor string, limit int) ([]model.Record, string, bool, error) {

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

	// 拼 SQL 查询：游标统一用 ::text 比较，兼容数值/TEXT/UUID 任意主键类型。
	// cursor 为空表示第一页（不加 WHERE）；否则按主键 ::text 的稳定全序做 keyset 分页。
	query := fmt.Sprintf(
		`SELECT row_to_json(t)::text FROM %s t ORDER BY %s::text LIMIT $1`,
		s.qualifiedName(), s.pk,
	)
	args := []any{limit}
	if cursor != "" {
		query = fmt.Sprintf(
			`SELECT row_to_json(t)::text FROM %s t WHERE %s::text > $1 ORDER BY %s::text LIMIT $2`,
			s.qualifiedName(), s.pk, s.pk,
		)
		args = []any{cursor, limit}
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, "", false, fmt.Errorf("查询源表失败: %w", err)
	}
	defer rows.Close()

	//  逐行转 Record
	records := make([]model.Record, 0, limit)
	var lastKey string
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, "", false, fmt.Errorf("读取行失败: %w", err)
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(raw), &row); err != nil {
			return nil, "", false, fmt.Errorf("解析行 JSON 失败: %w", err)
		}
		rec := s.rowToRecord(row)
		lastKey = rec.ID
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, "", false, fmt.Errorf("遍历行失败: %w", err)
	}

	// ⑤ 判断是否还有下一页
	hasMore := len(records) == limit
	// 即使最后一页（hasMore=false）也返回末行 ID 作游标：让引擎把最后一页一并 checkpoint。
	nextCursor := lastKey
	return records, nextCursor, hasMore, nil

}

// rowToRecord 把一行 JSON 转成 Record。
// ID 取主键列的值；Version/UpdatedAt 优先取 updated_at 列（RFC3339），没有则为 0/零值。
func (s *Source) rowToRecord(row map[string]any) model.Record {
	idVal, _ := row[s.pk]
	rec := model.Record{
		ID:   fmt.Sprintf("%v", idVal),
		Data: mustJSON(row),
	}
	// 用 RFC3339Nano 而非 RFC3339：Postgres timestamptz 默认带微秒，
	// RFC3339 解析带小数秒的时间必失败，会导致 Version 恒为 0（对账退化到 hash 比较）。
	if tsStr, ok := row["updated_at"].(string); ok {
		if t, err := time.Parse(time.RFC3339Nano, tsStr); err == nil {
			rec.UpdatedAt = t
			rec.Version = int(t.Unix())
		}
	}
	return rec
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
