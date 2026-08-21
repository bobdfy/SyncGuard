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
	dbURL string
	table string
	pk    string
	pool  *pgxpool.Pool
}

var rule = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func parseContent(syncContent string) (table, pk string, err error) {
	content := strings.SplitN(syncContent, ":", 2)
	table = content[0]
	pk = "id"
	if len(content) == 2 {
		pk = content[1]
	}

	if !rule.MatchString(table) {
		return "", "", fmt.Errorf("非法表名: %v, err: %w", table, engine.ErrNonRetryable)
	}
	if !rule.MatchString(pk) {
		return "", "", fmt.Errorf("非法主键: %v, err: %w", pk, engine.ErrNonRetryable)
	}
	return table, pk, nil
}

func NewSource(ctx context.Context, dbURL string, syncContent string) (*Source, error) {
	table, pk, err := parseContent(syncContent)
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

	return &Source{dbURL: dbURL, table: table, pk: pk, pool: pool}, nil
}

// Close 释放连接池。调用方（Worker/Reconciler）用完 Source 后必须 defer 关闭，
// 否则每次同步/对账都会泄漏一个连接池（每个池持有自己的连接，反复跑会连接耗尽）。
func (s *Source) Close() error {
	if s.pool != nil {
		s.pool.Close()
	}
	return nil
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
		s.table, s.pk,
	)
	args := []any{limit}
	if cursor != "" {
		query = fmt.Sprintf(
			`SELECT row_to_json(t)::text FROM %s t WHERE %s::text > $1 ORDER BY %s::text LIMIT $2`,
			s.table, s.pk, s.pk,
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
	nextCursor := ""
	if hasMore {
		nextCursor = lastKey
	}
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
