package postgres

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/bobdfy/syncguard/internal/model"
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

// TestSaveUpsertIntegration 真实 PG 往返：建标准目标表 → 两批 Save → 验证幂等 upsert。
func TestSaveUpsertIntegration(t *testing.T) {
	ctx := context.Background()
	dbURL := testDBURL(t)

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Skipf("本地 PG 不可用，跳过集成测试: %v", err)
	}
	defer pool.Close()
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		t.Skipf("本地 PG 不可用，跳过集成测试: %v", err)
	}

	// 建标准目标表（与文档约定一致：id TEXT PK, version INT, updated_at TIMESTAMPTZ, data JSONB）
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS test_sync_dst`); err != nil {
		t.Fatalf("清理失败: %v", err)
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE test_sync_dst (
		id TEXT PRIMARY KEY,
		version INT,
		updated_at TIMESTAMPTZ,
		data JSONB
	)`); err != nil {
		t.Fatalf("建表失败: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DROP TABLE IF EXISTS test_sync_dst`)
	})

	dst, err := NewDestination(ctx, dbURL, "test_sync_dst")
	if err != nil {
		t.Fatalf("NewDestination 失败: %v", err)
	}
	defer dst.Close()

	ts := time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC)
	rec := func(id string, version int, data string) model.Record {
		return model.Record{ID: id, Version: version, UpdatedAt: ts, Data: []byte(data)}
	}

	// 第一批：2 条
	if err := dst.Save(ctx, []model.Record{rec("a", 1, `{"v":1}`), rec("b", 1, `{"v":1}`)}); err != nil {
		t.Fatalf("第一批 Save 失败: %v", err)
	}

	// 第二批：1 条重复（a 升级）+ 1 条新（c）
	if err := dst.Save(ctx, []model.Record{rec("a", 2, `{"v":2}`), rec("c", 1, `{"v":1}`)}); err != nil {
		t.Fatalf("第二批 Save 失败: %v", err)
	}

	// 验证行数与内容
	var count int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM test_sync_dst`).Scan(&count); err != nil {
		t.Fatalf("COUNT 查询失败: %v", err)
	}
	if count != 3 {
		t.Fatalf("upsert 后期望 3 行，实际 %d 行", count)
	}

	var version int
	var data []byte
	if err := pool.QueryRow(ctx, `SELECT version, data FROM test_sync_dst WHERE id = 'a'`).Scan(&version, &data); err != nil {
		t.Fatalf("查询 a 失败: %v", err)
	}
	if version != 2 {
		t.Errorf("a 期望 version=2（被覆盖），实际 %d", version)
	}
	if !bytes.Equal(data, []byte(`{"v":2}`)) {
		t.Errorf("a 期望 data 被覆盖为 {\"v\":2}，实际 %s", data)
	}
}

// TestSaveEmptyRecordsIntegration 空批次直接成功，不写任何行。
func TestSaveEmptyRecordsIntegration(t *testing.T) {
	ctx := context.Background()
	d := &Destination{}
	if err := d.Save(ctx, nil); err != nil {
		t.Fatalf("空批次期望成功，实际: %v", err)
	}
}
