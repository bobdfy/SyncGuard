package repository

import (
	"context"
	"os"
	"testing"
	"time"
)

// testDB 连接本地 PostgreSQL 跑集成测试；连不上就 Skip（与 lock/circuitbreaker 测试同风格）。
// 连接串可用环境变量 SYNCGUARD_TEST_DB_URL 覆盖。
func testDB(t *testing.T) *DB {
	t.Helper()
	url := os.Getenv("SYNCGUARD_TEST_DB_URL")
	if url == "" {
		url = "postgres://postgres:syncguard@localhost:5432/postgres?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	db, err := NewDB(ctx, url)
	if err != nil {
		t.Skipf("本地 PG 不可用，跳过集成测试: %v", err)
	}
	t.Cleanup(db.Close)
	return db
}

// TestOutboxStoreRoundTrip 验证 outbox 全链路：Insert → ListUnsent 可见 → MarkSent 后不可见。
func TestOutboxStoreRoundTrip(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	// 清理：测试前后都清空，避免历史数据干扰
	_, err := db.Pool().Exec(ctx, `DELETE FROM outbox`)
	if err != nil {
		t.Fatalf("清理 outbox 表失败（确认已执行迁移 002_outbox.sql）: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.Pool().Exec(context.Background(), `DELETE FROM outbox`)
	})

	store := NewOutboxStore(db)
	payload := []byte(`{"job_id":1,"task_name":"sync_users","connection_id":1,"attempt":0,"delay_ms":0}`)

	// Insert：必须在事务里（业务写库与 outbox 同生共死的契约）
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin 失败: %v", err)
	}
	if err := store.Insert(ctx, tx, payload); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("Insert 失败: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit 失败: %v", err)
	}

	// ListUnsent 能看到
	msgs, err := store.ListUnsent(ctx, 10)
	if err != nil {
		t.Fatalf("ListUnsent 失败: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("期望 1 条未发送消息，实际 %d 条", len(msgs))
	}
	if string(msgs[0].Payload) != string(payload) {
		t.Errorf("payload 不一致: 期望 %s，实际 %s", payload, msgs[0].Payload)
	}

	// MarkSent 后不可见
	if err := store.MarkSent(ctx, msgs[0].ID); err != nil {
		t.Fatalf("MarkSent 失败: %v", err)
	}
	msgs, err = store.ListUnsent(ctx, 10)
	if err != nil {
		t.Fatalf("第二次 ListUnsent 失败: %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("MarkSent 后期望 0 条未发送，实际 %d 条", len(msgs))
	}
}
