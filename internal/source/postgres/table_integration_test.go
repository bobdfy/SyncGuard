package postgres

import (
	"context"
	"testing"
)

// TestTableSchemaAndFetchRowsIntegration 真实 PG 往返：读表结构 + 分页取原生行。
func TestTableSchemaAndFetchRowsIntegration(t *testing.T) {
	ctx := context.Background()
	dbURL := testDBURL(t)

	pool, err := newTestPool(ctx, dbURL)
	if err != nil {
		t.Skipf("本地 PG 不可用，跳过集成测试: %v", err)
	}
	defer pool.Close()

	// 建临时源表（含多种类型）
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS test_table_src`); err != nil {
		t.Fatalf("清理失败: %v", err)
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE test_table_src (
		id INTEGER PRIMARY KEY,
		name VARCHAR(50),
		price NUMERIC(8,2),
		flag BOOLEAN
	)`); err != nil {
		t.Fatalf("建表失败: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DROP TABLE IF EXISTS test_table_src`)
	})

	// 插 3 行
	for i := 1; i <= 3; i++ {
		if _, err := pool.Exec(ctx, `INSERT INTO test_table_src (id, name, price, flag) VALUES ($1, $2, $3, $4)`,
			i, "n"+string(rune('a'+i-1)), float64(i)+0.5, i%2 == 0); err != nil {
			t.Fatalf("插入失败: %v", err)
		}
	}

	src, err := NewSource(ctx, dbURL, "test_table_src")
	if err != nil {
		t.Fatalf("NewSource 失败: %v", err)
	}
	defer src.Close()

	// 结构：4 列 + 单列主键 id
	schema, err := src.TableSchema(ctx)
	if err != nil {
		t.Fatalf("TableSchema 失败: %v", err)
	}
	if schema.TableName != "test_table_src" {
		t.Errorf("期望表名 test_table_src，实际 %s", schema.TableName)
	}
	if len(schema.Columns) != 4 {
		t.Fatalf("期望 4 列，实际 %d", len(schema.Columns))
	}
	if len(schema.PrimaryKey) != 1 || schema.PrimaryKey[0] != "id" {
		t.Fatalf("期望单列主键 id，实际 %v", schema.PrimaryKey)
	}

	// 分页：limit=2 → 第一页 2 行 + hasMore，第二页 1 行
	rows, cursor, hasMore, err := src.FetchRows(ctx, "", 2)
	if err != nil {
		t.Fatalf("第一页 FetchRows 失败: %v", err)
	}
	if len(rows) != 2 || !hasMore || cursor != "2" {
		t.Fatalf("第一页期望 2 行 hasMore=true cursor=2，实际 len=%d hasMore=%v cursor=%q", len(rows), hasMore, cursor)
	}
	rows2, _, hasMore2, err := src.FetchRows(ctx, cursor, 2)
	if err != nil {
		t.Fatalf("第二页 FetchRows 失败: %v", err)
	}
	if len(rows2) != 1 || hasMore2 {
		t.Fatalf("第二页期望 1 行 hasMore=false，实际 len=%d hasMore=%v", len(rows2), hasMore2)
	}

	// 原生值类型保真：id 解码为整型（若走 row_to_json 会变成 float64）
	id0 := rows[0].Values[0]
	switch id0.(type) {
	case int32, int64, int16:
		// 期望的整型解码
	default:
		t.Errorf("期望 id 解码为整型，实际 %T", id0)
	}
}
