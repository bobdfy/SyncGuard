package postgres

import (
	"errors"
	"testing"
	"time"

	"github.com/bobdfy/syncguard/internal/engine"
)

// TestParseContent 验证 `表名[:主键列]` 的解析规则。
func TestParseContent(t *testing.T) {
	tests := []struct {
		name        string
		syncContent string
		wantTable   string
		wantPK      string
		wantErr     bool
	}{
		{name: "只有表名，主键默认 id", syncContent: "users", wantTable: "users", wantPK: "id"},
		{name: "表名+主键", syncContent: "orders:order_id", wantTable: "orders", wantPK: "order_id"},
		{name: "带下划线表名", syncContent: "user_profiles:uid", wantTable: "user_profiles", wantPK: "uid"},
		{name: "非法主键（连字符）", syncContent: "users:bad-name", wantErr: true},
		{name: "非法表名（连字符）", syncContent: "bad-name", wantErr: true},
		{name: "空串", syncContent: "", wantErr: true},
		{name: "数字开头表名", syncContent: "1table", wantErr: true},
		{name: "主键含分号注入", syncContent: "users:1;DROP", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table, pk, err := parseContent(tt.syncContent)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("期望报错，实际成功: table=%q pk=%q", table, pk)
				}
				// 配置错误属于永久错误，必须标记 ErrNonRetryable（Worker 才会进 DLQ 而非退避重试）
				if !errors.Is(err, engine.ErrNonRetryable) {
					t.Errorf("期望错误包装 ErrNonRetryable，实际: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("期望成功，实际报错: %v", err)
			}
			if table != tt.wantTable || pk != tt.wantPK {
				t.Errorf("期望 (%q, %q)，实际 (%q, %q)", tt.wantTable, tt.wantPK, table, pk)
			}
		})
	}
}

// TestRowToRecord 验证一行 JSON → Record 的映射。
func TestRowToRecord(t *testing.T) {
	src := &Source{pk: "id"}

	t.Run("正常映射：ID + updated_at 解析", func(t *testing.T) {
		ts := "2026-08-19T10:00:00.123456+08:00"
		want, err := time.Parse(time.RFC3339Nano, ts)
		if err != nil {
			t.Fatalf("测试用例时间解析失败: %v", err)
		}

		rec := src.rowToRecord(map[string]any{
			"id":         "user_1",
			"updated_at": ts,
			"name":       "张三",
		})

		if rec.ID != "user_1" {
			t.Errorf("期望 ID=user_1，实际 %q", rec.ID)
		}
		if !rec.UpdatedAt.Equal(want) {
			t.Errorf("期望 UpdatedAt=%v，实际 %v", want, rec.UpdatedAt)
		}
		if rec.Version != int(want.Unix()) {
			t.Errorf("期望 Version=%d，实际 %d", want.Unix(), rec.Version)
		}
	})

	t.Run("没有 updated_at 列 → Version 0、零值时间", func(t *testing.T) {
		rec := src.rowToRecord(map[string]any{
			"id":   "user_2",
			"name": "李四",
		})
		if rec.ID != "user_2" {
			t.Errorf("期望 ID=user_2，实际 %q", rec.ID)
		}
		if rec.Version != 0 {
			t.Errorf("期望 Version=0，实际 %d", rec.Version)
		}
		if !rec.UpdatedAt.IsZero() {
			t.Errorf("期望 UpdatedAt 零值，实际 %v", rec.UpdatedAt)
		}
	})

	t.Run("updated_at 不是合法时间 → 不 panic，Version=0", func(t *testing.T) {
		rec := src.rowToRecord(map[string]any{
			"id":         "user_3",
			"updated_at": "not-a-time",
		})
		if rec.ID != "user_3" {
			t.Errorf("期望 ID=user_3，实际 %q", rec.ID)
		}
		if rec.Version != 0 {
			t.Errorf("期望 Version=0，实际 %d", rec.Version)
		}
	})

	t.Run("主键列缺失 → 不 panic，ID 为字符串化的 nil", func(t *testing.T) {
		rec := src.rowToRecord(map[string]any{"name": "王五"})
		// 生产行为：fmt.Sprintf("%v", nil) 得到 "<nil>"（数据行必然有主键，此路径仅防御）
		if rec.ID != "<nil>" {
			t.Errorf("期望 ID=\"<nil>\"，实际 %q", rec.ID)
		}
	})
}
