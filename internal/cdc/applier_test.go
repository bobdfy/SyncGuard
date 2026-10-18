package cdc

import (
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

// TestRelationFromMessage 验证 RelationMessage → Relation 的精简提取（含主键标记）。
func TestRelationFromMessage(t *testing.T) {
	rel := &pglogrepl.RelationMessage{
		Namespace:    "public",
		RelationName: "orders",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: pgtype.Int4OID, Flags: 1}, // 1 = key 列
			{Name: "name", DataType: pgtype.TextOID, Flags: 0},
		},
	}
	got := relationFromMessage(rel)
	if got == nil || len(got.Columns) != 2 {
		t.Fatalf("期望 2 列，实际 %+v", got)
	}
	if !got.Columns[0].IsKey {
		t.Errorf("id 应标记为主键列")
	}
	if got.Columns[1].IsKey {
		t.Errorf("name 不应标记为主键列")
	}
	if got.Columns[0].TypeOID != pgtype.Int4OID {
		t.Errorf("id 的 TypeOID 期望 %d，实际 %d", pgtype.Int4OID, got.Columns[0].TypeOID)
	}
}

// TestBuildCreateDDL 验证自动建表 DDL：schema 限定、列类型、复合主键、标识符 Sanitize。
func TestBuildCreateDDL(t *testing.T) {
	rel := &Relation{Columns: []RelationColumn{
		{Name: "id", TypeOID: pgtype.Int4OID, IsKey: true},
		{Name: "tenant_id", TypeOID: pgtype.Int4OID, IsKey: true},
		{Name: "name", TypeOID: pgtype.TextOID},
	}}
	ddl := buildCreateDDL("public", "orders", rel)
	for _, want := range []string{
		`CREATE TABLE IF NOT EXISTS "public"."orders"`,
		`"id" int4`,
		`"tenant_id" int4`,
		`"name" text`,
		`PRIMARY KEY ("id", "tenant_id")`,
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("DDL 缺 %q，实际: %s", want, ddl)
		}
	}
}

// TestBuildCreateDDLNoPK 无主键列时不产生非法 PRIMARY KEY 子句。
func TestBuildCreateDDLNoPK(t *testing.T) {
	rel := &Relation{Columns: []RelationColumn{{Name: "v", TypeOID: pgtype.TextOID}}}
	ddl := buildCreateDDL("public", "t", rel)
	if strings.Contains(ddl, "PRIMARY KEY") {
		t.Errorf("无主键列不应有 PRIMARY KEY，实际: %s", ddl)
	}
}

// TestOidTypeName 验证 OID → 类型名映射与未知 OID 退回 text。
func TestOidTypeName(t *testing.T) {
	if got := oidTypeName(pgtype.Int4OID); got != "int4" {
		t.Errorf("Int4OID 期望 int4，实际 %q", got)
	}
	if got := oidTypeName(pgtype.TextOID); got != "text" {
		t.Errorf("TextOID 期望 text，实际 %q", got)
	}
	if got := oidTypeName(999999); got != "text" {
		t.Errorf("未知 OID 期望退回 text，实际 %q", got)
	}
}

// TestParsePayloadNewFormat 新格式（列值 + relation 元数据）。
func TestParsePayloadNewFormat(t *testing.T) {
	payload := []byte(`{"columns":[{"name":"id","value":1}],"relation":{"columns":[{"name":"id","type_oid":23,"is_key":true}]}}`)
	cols, rel, err := parsePayload(payload)
	if err != nil {
		t.Fatalf("解析新格式失败: %v", err)
	}
	if len(cols) != 1 || cols[0].Name != "id" {
		t.Errorf("列值解析错误: %+v", cols)
	}
	if rel == nil || len(rel.Columns) != 1 || !rel.Columns[0].IsKey {
		t.Errorf("relation 解析错误: %+v", rel)
	}
}

// TestParsePayloadOldFormat 旧格式（纯列数组）向后兼容，relation 为 nil。
func TestParsePayloadOldFormat(t *testing.T) {
	payload := []byte(`[{"name":"id","value":1}]`)
	cols, rel, err := parsePayload(payload)
	if err != nil {
		t.Fatalf("解析旧格式失败: %v", err)
	}
	if len(cols) != 1 || cols[0].Name != "id" {
		t.Errorf("列值解析错误: %+v", cols)
	}
	if rel != nil {
		t.Errorf("旧格式 relation 应为 nil，实际 %+v", rel)
	}
}
