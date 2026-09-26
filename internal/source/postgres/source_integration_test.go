package postgres

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// testDBURL 集成测试用的 PG 连接串；连不上就 Skip。
func testDBURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("SYNCGUARD_TEST_DB_URL")
	if url == "" {
		url = "postgres://postgres:syncguard@localhost:5432/postgres?sslmode=disable"
	}
	return url
}

// newTestPool 建连接池并 Ping，失败返回错误（调用方决定 Skip）。
func newTestPool(ctx context.Context, dbURL string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// TestFetchPaginationIntegration 真实 PG 往返：建临时源表 → 分页 Fetch。
func TestFetchPaginationIntegration(t *testing.T) {
	ctx := context.Background()
	dbURL := testDBURL(t)

	pool, err := newTestPool(ctx, dbURL)
	if err != nil {
		t.Skipf("本地 PG 不可用，跳过集成测试: %v", err)
	}
	defer pool.Close()

	// 建临时源表 + 插 3 行（先删后建保证干净）
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS test_sync_src`); err != nil {
		t.Fatalf("清理失败: %v", err)
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE test_sync_src (
		id TEXT PRIMARY KEY,
		updated_at TIMESTAMPTZ,
		name TEXT
	)`); err != nil {
		t.Fatalf("建表失败: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DROP TABLE IF EXISTS test_sync_src`)
	})
	for _, row := range [][2]string{
		{"1", "张三"}, {"2", "李四"}, {"3", "王五"},
	} {
		if _, err := pool.Exec(ctx,
			`INSERT INTO test_sync_src (id, updated_at, name) VALUES ($1, $2, $3)`,
			row[0], time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC), row[1]); err != nil {
			t.Fatalf("插入失败: %v", err)
		}
	}

	src, err := NewSource(ctx, dbURL, "test_sync_src")
	if err != nil {
		t.Fatalf("NewSource 失败: %v", err)
	}
	defer src.Close()

	// 第一页：limit=2 → 2 条 + hasMore
	records, cursor, hasMore, err := src.Fetch(ctx, "", 2)
	if err != nil {
		t.Fatalf("第一页 Fetch 失败: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("第一页期望 2 条，实际 %d 条", len(records))
	}
	if !hasMore {
		t.Errorf("第一页期望 hasMore=true")
	}
	if cursor != "2" {
		t.Errorf("期望游标=2，实际 %q", cursor)
	}
	if records[0].ID != "1" || records[1].ID != "2" {
		t.Errorf("期望按主键排序 [1,2]，实际 [%s,%s]", records[0].ID, records[1].ID)
	}
	if records[0].Version == 0 {
		t.Errorf("期望 updated_at 解析出非零 Version")
	}
	if !strings.Contains(string(records[0].Data), "张三") {
		t.Errorf("期望 Data 包含 name=张三，实际 %s", records[0].Data)
	}

	// 第二页：从游标继续 → 1 条 + hasMore=false
	records2, cursor2, hasMore2, err := src.Fetch(ctx, cursor, 2)
	if err != nil {
		t.Fatalf("第二页 Fetch 失败: %v", err)
	}
	if len(records2) != 1 || records2[0].ID != "3" {
		t.Fatalf("第二页期望 1 条(id=3)，实际 %+v", records2)
	}
	if hasMore2 {
		t.Errorf("第二页期望 hasMore=false")
	}
	if cursor2 != "" {
		t.Errorf("末页期望游标为空，实际 %q", cursor2)
	}

	// limit<=0 → 报错
	if _, _, _, err := src.Fetch(ctx, "", 0); err == nil {
		t.Errorf("limit=0 期望报错，实际成功")
	}
}

// TestFetchRejectsBadContent 非法 syncContent 在连库前就被拒绝。
func TestFetchRejectsBadContent(t *testing.T) {
	if _, err := NewSource(context.Background(), "postgres://x", "bad-name"); err == nil {
		t.Fatal("期望非法表名被拒绝，实际成功")
	}
}
