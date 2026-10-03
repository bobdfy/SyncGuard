package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/bobdfy/syncguard/internal/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestTableMirrorRoundTrip 真实 PG 往返：按源结构建同构表 → 列对列写入 → 验证类型保真。
//
// schema 手工构造（与 source/postgres.TableSchema 内省的 format_type 结果一致），
// 源表行用 pool 直接读原生值（等价于 FetchRows 内部的 rows.Values()），
// 二者对齐后交给 EnsureTable + SaveRows，验证目标表结构与数据都保真。
func TestTableMirrorRoundTrip(t *testing.T) {
	ctx := context.Background()
	dbURL := testDBURL(t)

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Skipf("本地 PG 不可用，跳过集成测试: %v", err)
	}
	defer pool.Close()
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		t.Skipf("本地 PG 不可用，跳过集成测试: %v", err)
	}

	// 建源表（模拟要被镜像的原表），含 numeric/jsonb/timestamptz 验证类型保真
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS mirror_src`); err != nil {
		t.Fatalf("清理失败: %v", err)
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE mirror_src (
		id BIGSERIAL PRIMARY KEY,
		name VARCHAR(100) NOT NULL,
		price NUMERIC(10,2),
		meta JSONB,
		created_at TIMESTAMPTZ
	)`); err != nil {
		t.Fatalf("建源表失败: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DROP TABLE IF EXISTS mirror_src`)
		_, _ = pool.Exec(context.Background(), `DROP TABLE IF EXISTS mirror_dst`)
	})

	if _, err := pool.Exec(ctx, `INSERT INTO mirror_src (name, price, meta, created_at) VALUES
		('alice', 12.34, '{"a":1}', '2026-08-19T10:00:00Z'),
		('bob', 99.99, '{"b":2}', '2026-08-20T10:00:00Z')`); err != nil {
		t.Fatalf("插入失败: %v", err)
	}

	// 源表结构（与 source/postgres.TableSchema 的 format_type 结果一致）
	schema := model.TableSchema{
		TableName: "mirror_dst",
		Columns: []model.Column{
			{Name: "id", DataType: "bigint", Nullable: false},
			{Name: "name", DataType: "character varying(100)", Nullable: false},
			{Name: "price", DataType: "numeric(10,2)", Nullable: true},
			{Name: "meta", DataType: "jsonb", Nullable: true},
			{Name: "created_at", DataType: "timestamp with time zone", Nullable: true},
		},
		PrimaryKey: []string{"id"},
	}

	// 目标建同构表
	dst, err := NewTableDestination(ctx, dbURL, "mirror_dst")
	if err != nil {
		t.Fatalf("NewTableDestination 失败: %v", err)
	}
	defer dst.Close()

	if err := dst.EnsureTable(ctx, schema); err != nil {
		t.Fatalf("EnsureTable 失败: %v", err)
	}

	// 读源表原生值（等价于 FetchRows 内部的 rows.Values()）
	rows, err := pool.Query(ctx, `SELECT * FROM mirror_src ORDER BY id`)
	if err != nil {
		t.Fatalf("查询源表失败: %v", err)
	}
	var all []model.RawRow
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			t.Fatalf("读取源表行值失败: %v", err)
		}
		all = append(all, model.RawRow{Values: values})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("遍历源表失败: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("期望读 2 行，实际 %d", len(all))
	}
	if err := dst.SaveRows(ctx, all); err != nil {
		t.Fatalf("SaveRows 失败: %v", err)
	}

	// 验证行数
	var count int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM mirror_dst`).Scan(&count); err != nil {
		t.Fatalf("COUNT 失败: %v", err)
	}
	if count != 2 {
		t.Fatalf("期望 2 行，实际 %d", count)
	}

	// 类型保真：numeric 精确、jsonb 原始、timestamptz 保持时间
	var price string
	if err := pool.QueryRow(ctx, `SELECT price::text FROM mirror_dst WHERE name='alice'`).Scan(&price); err != nil {
		t.Fatalf("查询 price 失败: %v", err)
	}
	if price != "12.34" {
		t.Errorf("期望 price=12.34（numeric 保真），实际 %s", price)
	}

	var aVal string
	if err := pool.QueryRow(ctx, `SELECT meta->>'a' FROM mirror_dst WHERE name='alice'`).Scan(&aVal); err != nil {
		t.Fatalf("查询 meta 失败: %v", err)
	}
	if aVal != "1" {
		t.Errorf("期望 meta.a=1（jsonb 保真），实际 %q", aVal)
	}

	var created time.Time
	if err := pool.QueryRow(ctx, `SELECT created_at FROM mirror_dst WHERE name='alice'`).Scan(&created); err != nil {
		t.Fatalf("查询 created_at 失败: %v", err)
	}
	if created.IsZero() {
		t.Errorf("期望 created_at 解析为时间（timestamptz 保真），实际零值")
	}

	// DDL 层类型保真：目标表列类型与源表一致
	var nameType, priceType string
	if err := pool.QueryRow(ctx, `SELECT format_type(a.atttypid, a.atttypmod)
		FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
		WHERE c.relname = 'mirror_dst' AND a.attname = 'name'`).Scan(&nameType); err != nil {
		t.Fatalf("查 name 类型失败: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT format_type(a.atttypid, a.atttypmod)
		FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
		WHERE c.relname = 'mirror_dst' AND a.attname = 'price'`).Scan(&priceType); err != nil {
		t.Fatalf("查 price 类型失败: %v", err)
	}
	if nameType != "character varying(100)" {
		t.Errorf("期望 name 类型 character varying(100)，实际 %s", nameType)
	}
	if priceType != "numeric(10,2)" {
		t.Errorf("期望 price 类型 numeric(10,2)，实际 %s", priceType)
	}
}
