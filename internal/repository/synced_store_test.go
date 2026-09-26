package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/bobdfy/syncguard/internal/model"
)

// 测试专用 userID / connectionID，避免污染真实数据。
const (
	testUserID       = 9990001
	testConnID       = 777
	testOtherConnID  = 778
	testOtherUserID  = 9990002
)

func testRecord(id string, version int, data string) model.Record {
	return model.Record{
		ID:        id,
		Version:   version,
		UpdatedAt: time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC),
		Data:      json.RawMessage(data),
	}
}

// TestSyncedStoreSaveAndQuery 验证 SaveWithUser 批量 upsert + 按用户/连接查询 + 分页。
func TestSyncedStoreSaveAndQuery(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	cleanup := func() {
		_, _ = db.Pool().Exec(ctx, `DELETE FROM synced_records WHERE user_id = $1 OR user_id = $2`, testUserID, testOtherUserID)
	}
	cleanup()
	t.Cleanup(cleanup)

	store := NewSyncedStore(db)

	// 写入 3 条（user=testUserID, conn=testConnID）
	records := []model.Record{
		testRecord("u1", 1, `{"name":"张三"}`),
		testRecord("u2", 2, `{"name":"李四"}`),
		testRecord("u3", 3, `{"name":"王五"}`),
	}
	if err := store.SaveWithUser(ctx, testUserID, testConnID, records); err != nil {
		t.Fatalf("SaveWithUser 失败: %v", err)
	}

	t.Run("幂等 upsert：同 key 重复写覆盖、行数不变", func(t *testing.T) {
		// 重复写 u1，改版本和数据
		if err := store.SaveWithUser(ctx, testUserID, testConnID, []model.Record{
			testRecord("u1", 9, `{"name":"张三改"}`),
		}); err != nil {
			t.Fatalf("第二次 SaveWithUser 失败: %v", err)
		}

		got, err := store.ListRecordsByUserAndConnection(ctx, testUserID, testConnID, 100, 0)
		if err != nil {
			t.Fatalf("查询失败: %v", err)
		}
		if len(got) != 3 {
			t.Fatalf("upsert 后期望仍 3 行，实际 %d 行", len(got))
		}
		for _, r := range got {
			if r.ID == "u1" {
				if r.Version != 9 {
					t.Errorf("u1 期望 version=9，实际 %d", r.Version)
				}
				if !bytes.Equal(r.Data, json.RawMessage(`{"name":"张三改"}`)) {
					t.Errorf("u1 期望内容已覆盖，实际 %s", r.Data)
				}
			}
		}
	})

	t.Run("按用户 + 连接过滤", func(t *testing.T) {
		got, err := store.ListRecordsByUserAndConnection(ctx, testUserID, testConnID, 100, 0)
		if err != nil {
			t.Fatalf("查询失败: %v", err)
		}
		if len(got) != 3 {
			t.Errorf("期望 3 条，实际 %d 条", len(got))
		}

		// 其他连接 → 0 条（多源隔离）
		other, err := store.ListRecordsByUserAndConnection(ctx, testUserID, testOtherConnID, 100, 0)
		if err != nil {
			t.Fatalf("查询失败: %v", err)
		}
		if len(other) != 0 {
			t.Errorf("其他连接期望 0 条，实际 %d 条", len(other))
		}

		// 其他用户 → 0 条（多用户隔离）
		otherUser, err := store.ListRecordsByUser(ctx, testOtherUserID, 100, 0)
		if err != nil {
			t.Fatalf("查询失败: %v", err)
		}
		if len(otherUser) != 0 {
			t.Errorf("其他用户期望 0 条，实际 %d 条", len(otherUser))
		}
	})

	t.Run("分页：limit=2 两页取完", func(t *testing.T) {
		page1, err := store.ListRecordsByUserAndConnection(ctx, testUserID, testConnID, 2, 0)
		if err != nil {
			t.Fatalf("第一页查询失败: %v", err)
		}
		if len(page1) != 2 {
			t.Errorf("第一页期望 2 条，实际 %d 条", len(page1))
		}
		page2, err := store.ListRecordsByUserAndConnection(ctx, testUserID, testConnID, 2, len(page1))
		if err != nil {
			t.Fatalf("第二页查询失败: %v", err)
		}
		if len(page2) != 1 {
			t.Errorf("第二页期望 1 条，实际 %d 条", len(page2))
		}

		// 两页合并 = 全集，无重复
		ids := map[string]bool{}
		for _, r := range append(page1, page2...) {
			if ids[r.ID] {
				t.Errorf("分页出现重复 ID: %s", r.ID)
			}
			ids[r.ID] = true
		}
		for _, want := range []string{"u1", "u2", "u3"} {
			if !ids[want] {
				t.Errorf("分页缺少 ID: %s", want)
			}
		}
	})
}

// TestSaveWithUserEmptyRecords 空批次直接成功。
func TestSaveWithUserEmptyRecords(t *testing.T) {
	db := testDB(t)
	store := NewSyncedStore(db)
	if err := store.SaveWithUser(context.Background(), testUserID, testConnID, nil); err != nil {
		t.Fatalf("空批次期望成功，实际: %v", err)
	}
}
