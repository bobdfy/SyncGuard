package model

// TableSchema 源表结构：目标端据此建同构表（列对列、类型保真）。
type TableSchema struct {
	TableName  string
	Columns    []Column // 按源表列序排列
	PrimaryKey []string // 主键列名（v1 仅支持单列主键）
}

// Column 一列的结构信息。
type Column struct {
	Name     string // 列名
	DataType string // 数据库原生完整类型，如 "character varying(255)"、"numeric(10,2)"、"timestamp with time zone"
	Nullable bool   // 是否允许 NULL
}

// RawRow 一行原始数据：Values 与 TableSchema.Columns 严格一一对应。
//
// 元素是驱动解码后的原生 Go 值（int64/float64/string/time.Time/[]byte/pgtype.Numeric…），
// 不经过 JSON 字符串化，因此能做到类型保真：numeric 不丢精度、timestamptz 保持时间类型、
// jsonb 保持原始字节。目标端按列类型重新编码写入，还原成原表结构。
type RawRow struct {
	Values []any
}
