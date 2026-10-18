package cdc

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

// 一条解析后的数据变更事件
type Event struct {
	// ConnectionID 是产生该事件的数据源连接，多租户据此路由到各自目标库
	ConnectionID int `json:"connection_id"`

	// LSN 是该事件在源库日志中的唯一位置标识
	LSN string `json:"lsn"`

	// Operation 是变更类型
	Operation string `json:"operation"`

	// Schema 是变更所属的 schema（命名空间，如 public），不是数据库名
	Schema string `json:"schema"`

	Table string `json:"table"`

	Columns []ColumnValue `json:"columns"`

	// Relation 是 reader 从 RelationMessage 提取的表结构元数据（列名 + 类型 OID + 是否主键列）。
	// 随事件一路传到 applier，供「目标表尚未建（快照没覆盖的新表）」时自动建同构表。
	// 老事件（本字段加入前投递的）为 nil，applier 对 nil 不做自动建表。
	Relation *Relation `json:"relation,omitempty"`
}

// 一列的值
type ColumnValue struct {
	Name  string `json:"name"`
	Value any    `json:"value,omitempty"`
	Null  bool   `json:"null,omitempty"`
}

// Relation 是一张表的精简结构元数据：只含建表所需的列名 + 类型 + 主键标记。
type Relation struct {
	Columns []RelationColumn `json:"columns"`
}

// RelationColumn 是 Relation 里的一列。
type RelationColumn struct {
	Name    string `json:"name"`
	TypeOID uint32 `json:"type_oid"`
	// IsKey 表示该列是 replica identity（通常是主键）的一部分。
	IsKey bool `json:"is_key"`
}

// Handler 处理一条变更事件
type Handler interface {
	Handle(ctx context.Context, ev Event) error
}

// 默认处理器
type LogHandler struct{}

func (LogHandler) Handle(ctx context.Context, ev Event) error {

	args := []any{"table", ev.Schema + "." + ev.Table, "lsn", ev.LSN}
	for _, c := range ev.Columns {
		if c.Null {
			args = append(args, c.Name, "<NULL>")
			continue
		}
		args = append(args, c.Name, c.Value)
	}
	slog.Info(ev.Operation, args...)
	return nil
}

// tupleToColumns 把二进制的行数据翻译成列值。关系未知或行数据为空返回 nil。
func tupleToColumns(rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) []ColumnValue {
	if rel == nil || tuple == nil {
		return nil
	}
	cols := make([]ColumnValue, 0, len(tuple.Columns))
	for i, c := range tuple.Columns {
		if i >= len(rel.Columns) {
			break
		}
		name := rel.Columns[i].Name
		switch c.DataType {
		case 'n': // NULL 值
			cols = append(cols, ColumnValue{Name: name, Null: true})
		case 'u': // 未变化的 TOAST，服务端没发内容 → 跳过，表示“不变”
			continue
		default:
			// 先统一按 string，类型映射（int/timestamp/jsonb…）留到下一步做
			cols = append(cols, ColumnValue{Name: name, Value: decodeColumnValue(rel.Columns[i].DataType, c.Data)})
		}
	}
	return cols
}

var typeMap = pgtype.NewMap()

// decodeColumnValue 按列的 OID 把文本格式的字节解码
func decodeColumnValue(oid uint32, data []byte) any {
	switch oid {
	case pgtype.NumericOID,
		pgtype.UUIDOID,
		pgtype.ByteaOID,    // bytea：保留 \x 十六进制文本
		pgtype.IntervalOID, // interval：保留原始文本
		pgtype.TimeOID:     // time（不带时区）：保留原始文本
		return string(data)
	case pgtype.JSONOID, pgtype.JSONBOID:
		return json.RawMessage(data)
	}

	typ, ok := typeMap.TypeForOID(oid)
	if !ok {
		return string(data)
	}
	v, err := typ.Codec.DecodeValue(typeMap, oid, pgtype.TextFormatCode, data)
	if err != nil {
		return string(data)
	}
	return v
}
