package reconciliation

import (
	"encoding/json"
	"testing"

	"github.com/bobdfy/syncguard/internal/model"
)

// rec 构造一条测试 Record。
func rec(id string, version int, data string) model.Record {
	return model.Record{
		ID:      id,
		Version: version,
		Data:    json.RawMessage(data),
	}
}

// findDiff 按 SourceID 找差异，找不到返回 nil。
func findDiff(diffs []model.Diff, id string) *model.Diff {
	for i := range diffs {
		if diffs[i].SourceID == id {
			return &diffs[i]
		}
	}
	return nil
}

// TestCompareMapsMissingInTarget 源端有、目标端缺失。
func TestCompareMapsMissingInTarget(t *testing.T) {
	src := map[string]model.Record{"1": rec("1", 3, `{"a":1}`)}
	diffs := compareMaps(src, map[string]model.Record{})

	if len(diffs) != 1 {
		t.Fatalf("期望 1 条差异，实际 %d 条", len(diffs))
	}
	d := diffs[0]
	if d.DiffType != "missing_in_target" {
		t.Errorf("期望 missing_in_target，实际 %s", d.DiffType)
	}
	if d.SourceVersion != 3 {
		t.Errorf("期望 SourceVersion=3，实际 %d", d.SourceVersion)
	}
	if d.SourceHash == "" {
		t.Errorf("期望 SourceHash 非空")
	}
}

// TestCompareMapsExtraInTarget 目标端有、源端缺失。
func TestCompareMapsExtraInTarget(t *testing.T) {
	tgt := map[string]model.Record{"2": rec("2", 1, `{"b":2}`)}
	diffs := compareMaps(map[string]model.Record{}, tgt)

	if len(diffs) != 1 {
		t.Fatalf("期望 1 条差异，实际 %d 条", len(diffs))
	}
	d := diffs[0]
	if d.DiffType != "extra_in_target" {
		t.Errorf("期望 extra_in_target，实际 %s", d.DiffType)
	}
	if d.TargetVersion != 1 {
		t.Errorf("期望 TargetVersion=1，实际 %d", d.TargetVersion)
	}
	if d.TargetHash == "" {
		t.Errorf("期望 TargetHash 非空")
	}
}

// TestCompareMapsVersionMismatch 版本号不一致。
func TestCompareMapsVersionMismatch(t *testing.T) {
	src := map[string]model.Record{"3": rec("3", 2, `{"c":3}`)}
	tgt := map[string]model.Record{"3": rec("3", 1, `{"c":3}`)}
	diffs := compareMaps(src, tgt)

	if len(diffs) != 1 {
		t.Fatalf("期望 1 条差异，实际 %d 条", len(diffs))
	}
	d := diffs[0]
	if d.DiffType != "version_mismatch" {
		t.Errorf("期望 version_mismatch，实际 %s", d.DiffType)
	}
	if d.SourceVersion != 2 || d.TargetVersion != 1 {
		t.Errorf("期望 源2/目标1，实际 源%d/目标%d", d.SourceVersion, d.TargetVersion)
	}
}

// TestCompareMapsContentMismatch 版本相同、内容 Hash 不同。
func TestCompareMapsContentMismatch(t *testing.T) {
	src := map[string]model.Record{"4": rec("4", 5, `{"a":1}`)}
	tgt := map[string]model.Record{"4": rec("4", 5, `{"a":2}`)}
	diffs := compareMaps(src, tgt)

	if len(diffs) != 1 {
		t.Fatalf("期望 1 条差异，实际 %d 条", len(diffs))
	}
	d := diffs[0]
	if d.DiffType != "content_mismatch" {
		t.Errorf("期望 content_mismatch，实际 %s", d.DiffType)
	}
	if d.SourceHash == d.TargetHash {
		t.Errorf("内容不同但 Hash 相同，测试用例失效")
	}
	if d.SourceHash == "" || d.TargetHash == "" {
		t.Errorf("期望 Hash 非空：源=%q 目标=%q", d.SourceHash, d.TargetHash)
	}
}

// TestCompareMapsIdenticalNoDiff 完全一致的数据不产生差异。
func TestCompareMapsIdenticalNoDiff(t *testing.T) {
	src := map[string]model.Record{"5": rec("5", 7, `{"x":1}`)}
	tgt := map[string]model.Record{"5": rec("5", 7, `{"x":1}`)}
	diffs := compareMaps(src, tgt)

	if len(diffs) != 0 {
		t.Fatalf("期望 0 条差异，实际 %d 条: %+v", len(diffs), diffs)
	}
}

// TestCompareMapsMixed 混合场景：四类差异各一条 + 一条一致，精确断言。
func TestCompareMapsMixed(t *testing.T) {
	src := map[string]model.Record{
		"missing": rec("missing", 1, `{"m":1}`),
		"version": rec("version", 9, `{"v":1}`),
		"content": rec("content", 3, `{"c":1}`),
		"same":    rec("same", 2, `{"s":1}`),
	}
	tgt := map[string]model.Record{
		"extra":   rec("extra", 4, `{"e":1}`),
		"version": rec("version", 8, `{"v":1}`),
		"content": rec("content", 3, `{"c":2}`),
		"same":    rec("same", 2, `{"s":1}`),
	}

	diffs := compareMaps(src, tgt)
	if len(diffs) != 4 {
		t.Fatalf("期望 4 条差异，实际 %d 条: %+v", len(diffs), diffs)
	}

	want := map[string]string{
		"missing": "missing_in_target",
		"extra":   "extra_in_target",
		"version": "version_mismatch",
		"content": "content_mismatch",
	}
	for id, typ := range want {
		d := findDiff(diffs, id)
		if d == nil {
			t.Errorf("期望找到差异 %s(%s)，实际没有", id, typ)
			continue
		}
		if d.DiffType != typ {
			t.Errorf("%s: 期望类型 %s，实际 %s", id, typ, d.DiffType)
		}
	}
	if findDiff(diffs, "same") != nil {
		t.Errorf("一致的数据不该出现在差异里")
	}
}
