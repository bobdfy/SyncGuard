package cdc

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// EventStore 绑定一个源连接：把 CDC 事件写进 cdc_events，并在同一事务里推进该连接的断点。
type EventStore struct {
	pool         *pgxpool.Pool
	connectionID int
	slotName     string
}

// NewEventStore 构造某连接的 EventStore。
func NewEventStore(pool *pgxpool.Pool, connectionID int, slotName string) *EventStore {
	return &EventStore{
		pool:         pool,
		connectionID: connectionID,
		slotName:     slotName,
	}
}

// Append 在一个事务里批量写入事件并推进断点位点。
// connection_id 与 lsn 一起构成去重键：同一连接内 lsn 唯一，不同连接 LSN 可能撞值。
func (e *EventStore) Append(ctx context.Context, events []Event, commitLSN string) error {
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("开启事务失败: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, ev := range events {
		payload, err := json.Marshal(eventPayload{Columns: ev.Columns, Relation: ev.Relation})
		if err != nil {
			return fmt.Errorf("序列化事件内容失败: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO cdc_events (connection_id, lsn, commit_lsn, operation, schema_name, table_name, payload)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (connection_id, lsn) DO NOTHING`,
			e.connectionID, ev.LSN, commitLSN, ev.Operation, ev.Schema, ev.Table, payload); err != nil {
			return fmt.Errorf("写入事件失败: %w", err)
		}
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO cdc_checkpoints (connection_id, slot_name, confirmed_lsn, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (connection_id, slot_name) DO UPDATE
			SET confirmed_lsn = EXCLUDED.confirmed_lsn, updated_at = now()`,
		e.connectionID, e.slotName, commitLSN); err != nil {
		return fmt.Errorf("更新断点失败: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("提交事务失败: %w", err)
	}
	return nil
}

// SaveCheckpoint 仅写入断点位点，定死位置（快照钉切点用）。
func (e *EventStore) SaveCheckpoint(ctx context.Context, lsn string) error {
	if _, err := e.pool.Exec(ctx, `
		INSERT INTO cdc_checkpoints (connection_id, slot_name, confirmed_lsn, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (connection_id, slot_name) DO UPDATE
			SET confirmed_lsn = EXCLUDED.confirmed_lsn, updated_at = now()`,
		e.connectionID, e.slotName, lsn); err != nil {
		return fmt.Errorf("写入断点失败: %w", err)
	}
	return nil
}

// LoadCheckpoint 读取该连接已落 outbox 的断点位点，没有则返回 ok=false。
func (e *EventStore) LoadCheckpoint(ctx context.Context) (string, bool, error) {
	var lsn string
	err := e.pool.QueryRow(ctx,
		"SELECT confirmed_lsn FROM cdc_checkpoints WHERE connection_id = $1 AND slot_name = $2",
		e.connectionID, e.slotName).Scan(&lsn)
	if err == pgx.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("查询断点失败: %w", err)
	}
	return lsn, true, nil
}

// StoredEvent 从 cdc_events 读出来的一条记录 + 其事件内容。
type StoredEvent struct {
	ID    int64
	Event Event
}

// eventPayload 是 cdc_events.payload 的 JSON 结构：列值 + 表结构元数据（可选）。
type eventPayload struct {
	Columns  []ColumnValue `json:"columns"`
	Relation *Relation     `json:"relation,omitempty"`
}

// parsePayload 解析 cdc_events.payload。新格式是 {"columns":[...],"relation":{...}}，
// 旧格式（relation 字段加入前）直接是列数组；据此兼容老事件，不因格式差异卡死 dispatcher。
func parsePayload(payload []byte) ([]ColumnValue, *Relation, error) {
	var p eventPayload
	if err := json.Unmarshal(payload, &p); err == nil {
		return p.Columns, p.Relation, nil
	}
	var cols []ColumnValue
	if err := json.Unmarshal(payload, &cols); err != nil {
		return nil, nil, fmt.Errorf("解析事件列值失败: %w", err)
	}
	return cols, nil, nil
}

// DispatchStore 给 dispatcher 用：全局扫 cdc_events 未分发事件并标记，不关心具体连接。
type DispatchStore struct {
	pool *pgxpool.Pool
}

// NewDispatchStore 构造全局分发读取器。
func NewDispatchStore(pool *pgxpool.Pool) *DispatchStore {
	return &DispatchStore{pool: pool}
}

// ListUnprocessed 读取未分发的事件（跨所有连接），按 id 升序，最多 limit 条。
func (s *DispatchStore) ListUnprocessed(ctx context.Context, limit int) ([]StoredEvent, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, connection_id, lsn, operation, schema_name, table_name, payload
		FROM cdc_events
		WHERE processed_at IS NULL
		ORDER BY id
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("查询未分发事件失败: %w", err)
	}
	defer rows.Close()

	out := make([]StoredEvent, 0, limit)
	for rows.Next() {
		var (
			id                int64
			connectionID      int
			lsn, op, sch, tbl string
			payload           []byte
		)
		if err := rows.Scan(&id, &connectionID, &lsn, &op, &sch, &tbl, &payload); err != nil {
			return nil, fmt.Errorf("扫描事件失败: %w", err)
		}
		cols, rel, err := parsePayload(payload)
		if err != nil {
			return nil, err
		}
		out = append(out, StoredEvent{
			ID: id,
			Event: Event{
				ConnectionID: connectionID,
				LSN:          lsn,
				Operation:    op,
				Schema:       sch,
				Table:        tbl,
				Columns:      cols,
				Relation:     rel,
			},
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历事件失败: %w", err)
	}
	return out, nil
}

// MarkProcessed 标记某条事件已分发。
func (s *DispatchStore) MarkProcessed(ctx context.Context, id int64) error {
	if _, err := s.pool.Exec(ctx,
		`UPDATE cdc_events SET processed_at = NOW() WHERE id = $1`, id); err != nil {
		return fmt.Errorf("标记事件已分发失败: %w", err)
	}
	return nil
}
