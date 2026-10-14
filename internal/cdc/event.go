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
	// LSN 是该事件在源库日志中的唯一位置标识
	LSN string `json:"lsn"`

	// Operation 是变更类型
	Operation string `json:"operation"`

	// Schema 是变更所属的数据库名
	Schema string `json:"schema"`

	Table string `json:"table"`

	Columns []ColumnValue `json:"columns"`
}

// 一列的值
type ColumnValue struct {
	Name  string `json:"name"`
	Value any    `json:"value,omitempty"`
	Null  bool   `json:"null,omitempty"`
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
