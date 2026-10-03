package engine_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/bobdfy/syncguard/internal/engine"
	"github.com/bobdfy/syncguard/internal/model"
)

// memTableSource 内存版 TableSource：预置 schema + rows，按索引分页。
type memTableSource struct {
	schema model.TableSchema
	rows   []model.RawRow
}

func (s *memTableSource) TableSchema(ctx context.Context) (model.TableSchema, error) {
	return s.schema, nil
}

func (s *memTableSource) FetchRows(ctx context.Context, cursor string, limit int) ([]model.RawRow, string, bool, error) {
	start := 0
	if cursor != "" {
		start, _ = strconv.Atoi(cursor)
	}
	end := start + limit
	if end > len(s.rows) {
		end = len(s.rows)
	}
	batch := s.rows[start:end]
	hasMore := end < len(s.rows)
	next := ""
	if hasMore {
		next = strconv.Itoa(end)
	}
	return batch, next, hasMore, nil
}

func (s *memTableSource) Close() error { return nil }

// memTableDest 内存版 TableDestination。
type memTableDest struct {
	ensured bool
	schema  model.TableSchema
	saved   int
}

func (d *memTableDest) EnsureTable(ctx context.Context, schema model.TableSchema) error {
	d.ensured = true
	d.schema = schema
	return nil
}

func (d *memTableDest) SaveRows(ctx context.Context, rows []model.RawRow) error {
	d.saved += len(rows)
	return nil
}

func (d *memTableDest) Close() error { return nil }

// memProgress 内存版 Progress。
type memProgress struct {
	cursor string
	status string
	total  int
}

func (p *memProgress) GetCheckpoint(ctx context.Context, taskName string) (string, error) {
	return p.cursor, nil
}

func (p *memProgress) UpdateCheckpoint(ctx context.Context, taskName string, cursor string) error {
	p.cursor = cursor
	return nil
}

func (p *memProgress) CreateBatch(ctx context.Context) (int64, error) {
	return 1, nil
}

func (p *memProgress) CompleteBatch(ctx context.Context, batchID int64, status string, totalCount int) error {
	p.status = status
	p.total = totalCount
	return nil
}

// failTableSource 会失败的镜像源：第一次返回 1 行，第二次返回 error。
type failTableSource struct {
	calls int
}

func (s *failTableSource) TableSchema(ctx context.Context) (model.TableSchema, error) {
	return model.TableSchema{}, nil
}

func (s *failTableSource) FetchRows(ctx context.Context, cursor string, limit int) ([]model.RawRow, string, bool, error) {
	s.calls++
	if s.calls == 1 {
		return []model.RawRow{{Values: []any{1}}}, "1", true, nil
	}
	return nil, "", false, fmt.Errorf("模拟数据源失败")
}

func (s *failTableSource) Close() error { return nil }

// newTableSource 生成 n 行的内存镜像源。
func newTableSource(n int) *memTableSource {
	rows := make([]model.RawRow, n)
	for i := range rows {
		rows[i] = model.RawRow{Values: []any{i + 1, "v" + strconv.Itoa(i+1)}}
	}
	return &memTableSource{
		schema: model.TableSchema{
			TableName:  "t",
			Columns:    []model.Column{{Name: "id"}, {Name: "val"}},
			PrimaryKey: []string{"id"},
		},
		rows: rows,
	}
}

// TestRunTableSyncsAllRows 完整镜像：建表 + 20 行分 2 页 + completed。
func TestRunTableSyncsAllRows(t *testing.T) {
	src := newTableSource(20)
	dst := &memTableDest{}
	progress := &memProgress{}

	if err := engine.RunTable(context.Background(), src, dst, progress, "task", 10); err != nil {
		t.Fatalf("RunTable 出错: %v", err)
	}
	if !dst.ensured {
		t.Errorf("期望调用 EnsureTable")
	}
	if dst.saved != 20 {
		t.Errorf("期望同步 20 行，实际 %d", dst.saved)
	}
	if progress.status != "completed" {
		t.Errorf("期望状态 completed，实际 %s", progress.status)
	}
	if progress.total != 20 {
		t.Errorf("期望 total=20，实际 %d", progress.total)
	}
}

// TestRunTableResumesFromCheckpoint 断点续传：游标停在 10，从第 11 行继续。
func TestRunTableResumesFromCheckpoint(t *testing.T) {
	src := newTableSource(20)
	dst := &memTableDest{}
	progress := &memProgress{cursor: "10"} // 前 10 行已同步

	if err := engine.RunTable(context.Background(), src, dst, progress, "task", 10); err != nil {
		t.Fatalf("RunTable 出错: %v", err)
	}
	if dst.saved != 10 {
		t.Errorf("期望续传 10 行，实际 %d", dst.saved)
	}
}

// TestRunTableMarksFailedOnError 中途失败 → 状态 failed。
func TestRunTableMarksFailedOnError(t *testing.T) {
	src := &failTableSource{}
	dst := &memTableDest{}
	progress := &memProgress{}

	err := engine.RunTable(context.Background(), src, dst, progress, "task", 10)
	if err == nil {
		t.Fatalf("期望 RunTable 返回错误")
	}
	if progress.status != "failed" {
		t.Errorf("期望状态 failed，实际 %s", progress.status)
	}
}
