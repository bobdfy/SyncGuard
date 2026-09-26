package postgres

import (
	"context"
	"testing"

	"github.com/bobdfy/syncguard/internal/model"
)

// TestNewDestinationRejectsBadTable 验证表名白名单：非法表名在连库之前就被拒绝。
// （校验先于 pgxpool.New，因此即使目标库不可达也会先报表名错误。）
func TestNewDestinationRejectsBadTable(t *testing.T) {
	bad := []string{
		"users;DROP TABLE users",
		"1table",
		"bad-name",
		"",
		"a b",
	}
	for _, table := range bad {
		t.Run("非法表名 "+table, func(t *testing.T) {
			_, err := NewDestination(context.Background(), "postgres://x", table)
			if err == nil {
				t.Fatalf("期望表名 %q 被拒绝，实际通过了校验", table)
			}
		})
	}
}

// TestSaveEmptyRecords 空批次直接成功，不触碰连接池。
func TestSaveEmptyRecords(t *testing.T) {
	d := &Destination{} // 没有 pool，验证 Save 对空输入直接返回

	if err := d.Save(context.Background(), nil); err != nil {
		t.Fatalf("Save(nil) 期望成功，实际: %v", err)
	}
	if err := d.Save(context.Background(), []model.Record{}); err != nil {
		t.Fatalf("Save(空切片) 期望成功，实际: %v", err)
	}
}
