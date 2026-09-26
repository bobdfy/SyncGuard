package engine_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/bobdfy/syncguard/internal/engine"
	"github.com/bobdfy/syncguard/internal/model"
	"github.com/bobdfy/syncguard/internal/source/mock"
)

// memDest 内存版 Destination，用于测试引擎，不依赖数据库。
// 它实现了 engine.Destination 接口，用 map 存数据（天然幂等：同 key 覆盖）。
type memDest struct {
	records map[string]model.Record
	cursor  string
	status  string
	total   int
}

func newMemDest() *memDest {
	return &memDest{records: make(map[string]model.Record)}
}

// failSource 会失败的数据源：第一次返回 1 条，第二次返回 error。
type failSource struct {
	calls int // 记录被调了几次
}

func (d *memDest) Save(ctx context.Context, records []model.Record) error {
	for _, r := range records {
		d.records[r.ID] = r // map 覆盖 = 幂等
	}
	return nil
}

func (d *memDest) GetCheckpoint(ctx context.Context, taskName string) (string, error) {
	return d.cursor, nil
}

func (d *memDest) UpdateCheckpoint(ctx context.Context, taskName string, cursor string) error {
	d.cursor = cursor
	return nil
}

func (d *memDest) CreateBatch(ctx context.Context) (int64, error) {
	return 1, nil
}

func (d *memDest) CompleteBatch(ctx context.Context, batchID int64, status string, totalCount int) error {
	d.status = status
	d.total = totalCount
	return nil
}

// Close 实现 engine.Destination 接口：无资源可释放。
func (d *memDest) Close() error { return nil }

func (s *failSource) Fetch(ctx context.Context, cursor string, limit int) ([]model.Record, string, bool, error) {
	s.calls++
	if s.calls == 1 {
		// 第一次：返回 1 条数据，hasMore=true（告诉引擎"还有下一页"）
		return []model.Record{{ID: "record_0001"}}, "record_0001", true, nil
	}
	// 第二次：直接报错
	return nil, "", false, fmt.Errorf("模拟数据源失败")
}

// Close 实现 engine.Source 接口：无资源可释放。
func (s *failSource) Close() error { return nil }

// 测试 1：完整同步 20 条，每页 10 条（共 2 页），全部落库、状态 completed。
func TestRunSyncsAllRecords(t *testing.T) {
	src := mock.NewGenerator(20)
	dst := newMemDest()
	eng := engine.New(src, dst, 10)

	if err := eng.Run(context.Background(), "test_task"); err != nil {
		t.Fatalf("Run 出错: %v", err)
	}

	if len(dst.records) != 20 {
		t.Errorf("期望同步 20 条，实际 %d 条", len(dst.records))
	}
	if dst.status != "completed" {
		t.Errorf("期望状态 completed，实际 %s", dst.status)
	}
	if dst.total != 20 {
		t.Errorf("期望 total=20，实际 %d", dst.total)
	}
}

// 测试 2：断点续传——游标停在 record_0010，重启后从第 11 条继续。
func TestRunResumesFromCheckpoint(t *testing.T) {
	src := mock.NewGenerator(20)
	dst := newMemDest()
	dst.cursor = "record_0010" // 模拟前 10 条已经同步过

	eng := engine.New(src, dst, 10)
	if err := eng.Run(context.Background(), "test_task"); err != nil {
		t.Fatalf("Run 出错: %v", err)
	}

	// 只应同步 record_0011 ~ record_0020 这 10 条
	if len(dst.records) != 10 {
		t.Errorf("期望同步 10 条，实际 %d 条", len(dst.records))
	}
	if dst.total != 10 {
		t.Errorf("期望 total=10，实际 %d", dst.total)
	}
	if _, ok := dst.records["record_0011"]; !ok {
		t.Errorf("期望从 record_0011 开始，但没找到它")
	}
}

// 测试 3：幂等——重复 Save 同一条，结果仍然只有一条。
func TestSaveIsIdempotent(t *testing.T) {
	dst := newMemDest()
	rec := model.Record{ID: "record_0001", Version: 1}

	if err := dst.Save(context.Background(), []model.Record{rec}); err != nil {
		t.Fatalf("第一次 Save 出错: %v", err)
	}
	if err := dst.Save(context.Background(), []model.Record{rec}); err != nil {
		t.Fatalf("第二次 Save 出错: %v", err)
	}

	if len(dst.records) != 1 {
		t.Errorf("重复写同一条，期望仍只有 1 条，实际 %d 条", len(dst.records))
	}
}

// 测试 4：同步中途失败 → 最终状态是 failed，而不是 completed。
func TestRunMarksFailedOnError(t *testing.T) {
	src := &failSource{} // 会失败的数据源
	dst := newMemDest()
	eng := engine.New(src, dst, 10)

	err := eng.Run(context.Background(), "test_task")

	if err == nil {
		t.Fatalf("期望 Run 返回错误，结果没有错误")
	}
	if dst.status != "failed" {
		t.Errorf("期望状态 failed，实际 %s", dst.status)
	}
}
