package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/bobdfy/syncguard/internal/engine"
	"github.com/bobdfy/syncguard/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// 编译期检查：TableDestination 实现了 engine.TableDestination 接口
var _ engine.TableDestination = (*TableDestination)(nil)

// TableDestination 数据库镜像目标：按源表结构建同构表 + 列对列写入。
// 与信封版 Destination 不同，它不写 (id,version,updated_at,data) 四列，而是原样还原源表结构。
type TableDestination struct {
	dbURL  string
	table  string
	pool   *pgxpool.Pool
	schema model.TableSchema // EnsureTable 时缓存，供 SaveRows 拼 SQL
}

// NewTableDestination 校验表名 + 建立到目标库的连接池。
func NewTableDestination(ctx context.Context, dbURL, table string) (*TableDestination, error) {
	if !identRe.MatchString(table) {
		return nil, fmt.Errorf("非法目标表名 %q（只允许字母/数字/下划线）", table)
	}
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return nil, fmt.Errorf("连接目标库失败: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("连接目标库失败: %w", err)
	}
	return &TableDestination{dbURL: dbURL, table: table, pool: pool}, nil
}

// Close 释放连接池。
func (d *TableDestination) Close() error {
	if d.pool != nil {
		d.pool.Close()
	}
	return nil
}

// EnsureTable 按源表结构在目标库建同构表（幂等），并缓存结构供 SaveRows 用。
func (d *TableDestination) EnsureTable(ctx context.Context, schema model.TableSchema) error {
	d.schema = schema
	ddl := buildCreateTableDDL(d.table, schema)
	if _, err := d.pool.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("TableDestination.EnsureTable: %w", err)
	}
	return nil
}

// SaveRows 列对列批量 upsert（幂等 + 原子）。
//
// 原子性：整个批次包在一个事务里，要么全成功、要么全失败，不留下半页数据
// （与 engine.TableDestination.SaveRows 的契约一致）。
func (d *TableDestination) SaveRows(ctx context.Context, rows []model.RawRow) error {
	if len(rows) == 0 {
		return nil
	}
	if len(d.schema.Columns) == 0 {
		return fmt.Errorf("SaveRows: 未初始化表结构（需先调 EnsureTable）")
	}

	sql := buildUpsertSQL(d.table, d.schema)

	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("TableDestination.SaveRows Begin: %w", err)
	}
	defer tx.Rollback(ctx) // Commit 成功后是 no-op

	batch := &pgx.Batch{}
	for _, row := range rows {
		if len(row.Values) != len(d.schema.Columns) {
			return fmt.Errorf("SaveRows: 行列数不匹配（%d 值 vs %d 列）", len(row.Values), len(d.schema.Columns))
		}
		batch.Queue(sql, row.Values...)
	}
	br := tx.SendBatch(ctx, batch)
	for range rows {
		if _, err := br.Exec(); err != nil {
			_ = br.Close()
			return fmt.Errorf("TableDestination.SaveRows batch exec: %w", err)
		}
	}
	if err := br.Close(); err != nil {
		return fmt.Errorf("TableDestination.SaveRows batch close: %w", err)
	}
	return tx.Commit(ctx)
}

// buildCreateTableDDL 依据表结构拼 CREATE TABLE IF NOT EXISTS 语句。
// 列名/表名用 Identifier.Sanitize 防注入；类型串来自源库 format_type，可直接内嵌。
func buildCreateTableDDL(table string, schema model.TableSchema) string {
	parts := make([]string, 0, len(schema.Columns)+1)
	for _, c := range schema.Columns {
		def := pgx.Identifier{c.Name}.Sanitize() + " " + c.DataType
		if !c.Nullable {
			def += " NOT NULL"
		}
		parts = append(parts, def)
	}
	if len(schema.PrimaryKey) > 0 {
		pk := make([]string, 0, len(schema.PrimaryKey))
		for _, p := range schema.PrimaryKey {
			pk = append(pk, pgx.Identifier{p}.Sanitize())
		}
		parts = append(parts, "PRIMARY KEY ("+strings.Join(pk, ", ")+")")
	}
	return "CREATE TABLE IF NOT EXISTS " + pgx.Identifier{table}.Sanitize() +
		" (" + strings.Join(parts, ", ") + ")"
}

// buildUpsertSQL 依据表结构拼列对列 upsert 语句：
//
//	INSERT INTO t (c1,c2) VALUES ($1,$2) ON CONFLICT (pk) DO UPDATE SET c1=EXCLUDED.c1,...
//
// 无主键、或除主键外无其它列时退化为 ON CONFLICT DO NOTHING。
func buildUpsertSQL(table string, schema model.TableSchema) string {
	colNames := make([]string, len(schema.Columns))
	placeholders := make([]string, len(schema.Columns))
	for i, c := range schema.Columns {
		colNames[i] = pgx.Identifier{c.Name}.Sanitize()
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	pkSet := make(map[string]bool, len(schema.PrimaryKey))
	for _, p := range schema.PrimaryKey {
		pkSet[p] = true
	}
	updateCols := make([]string, 0, len(schema.Columns))
	for _, c := range schema.Columns {
		if !pkSet[c.Name] {
			updateCols = append(updateCols, pgx.Identifier{c.Name}.Sanitize()+" = EXCLUDED."+pgx.Identifier{c.Name}.Sanitize())
		}
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		pgx.Identifier{table}.Sanitize(),
		strings.Join(colNames, ", "),
		strings.Join(placeholders, ", "))

	if len(schema.PrimaryKey) == 0 || len(updateCols) == 0 {
		sql += " ON CONFLICT DO NOTHING"
	} else {
		pkNames := make([]string, len(schema.PrimaryKey))
		for i, p := range schema.PrimaryKey {
			pkNames[i] = pgx.Identifier{p}.Sanitize()
		}
		sql += fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET %s",
			strings.Join(pkNames, ", "), strings.Join(updateCols, ", "))
	}
	return sql
}
