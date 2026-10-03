package postgres

import (
	"strings"
	"testing"

	"github.com/bobdfy/syncguard/internal/model"
)

// TestBuildCreateTableDDL 验证建表 DDL：列名引用、类型内嵌、NOT NULL、主键。
func TestBuildCreateTableDDL(t *testing.T) {
	schema := model.TableSchema{
		TableName: "orders",
		Columns: []model.Column{
			{Name: "id", DataType: "integer", Nullable: false},
			{Name: "name", DataType: "character varying(255)", Nullable: true},
		},
		PrimaryKey: []string{"id"},
	}
	ddl := buildCreateTableDDL("orders", schema)

	if !strings.Contains(ddl, `"orders"`) {
		t.Errorf("期望包含表名，实际: %s", ddl)
	}
	if !strings.Contains(ddl, `"id" integer NOT NULL`) {
		t.Errorf("期望 id 列 NOT NULL，实际: %s", ddl)
	}
	if !strings.Contains(ddl, `"name" character varying(255)`) {
		t.Errorf("期望 name 列含类型，实际: %s", ddl)
	}
	if !strings.Contains(ddl, `PRIMARY KEY ("id")`) {
		t.Errorf("期望主键，实际: %s", ddl)
	}
}

// TestBuildUpsertSQL 验证列对列 upsert：非主键列覆盖、冲突目标为主键。
func TestBuildUpsertSQL(t *testing.T) {
	schema := model.TableSchema{
		TableName: "orders",
		Columns: []model.Column{
			{Name: "id", DataType: "integer", Nullable: false},
			{Name: "name", DataType: "text", Nullable: true},
		},
		PrimaryKey: []string{"id"},
	}
	sql := buildUpsertSQL("orders", schema)

	if !strings.Contains(sql, `INSERT INTO "orders" ("id", "name") VALUES ($1, $2)`) {
		t.Errorf("期望列对列 INSERT，实际: %s", sql)
	}
	if !strings.Contains(sql, `ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name"`) {
		t.Errorf("期望冲突更新非主键列，实际: %s", sql)
	}
}

// TestBuildUpsertSQLNoNonPKCols 仅主键列 → 退化为 DO NOTHING（不产生非法空 SET）。
func TestBuildUpsertSQLNoNonPKCols(t *testing.T) {
	schema := model.TableSchema{
		TableName: "t",
		Columns: []model.Column{
			{Name: "id", DataType: "integer", Nullable: false},
		},
		PrimaryKey: []string{"id"},
	}
	sql := buildUpsertSQL("t", schema)
	if !strings.Contains(sql, "ON CONFLICT DO NOTHING") {
		t.Errorf("仅主键列时应退化为 DO NOTHING，实际: %s", sql)
	}
}
