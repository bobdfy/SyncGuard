package cdc

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// EventStore 把 CDC 事件写入 cdc_events 表，并在同一个事务里推进 cdc_checkpoints 位点。
type EventStore struct {
	pool     *pgxpool.Pool
	slotName string
}

// 用普通的连接池跑事件落库
func NewEventStore(pool *pgxpool.Pool, slotName string) *EventStore {
	return &EventStore{
		pool:     pool,
		slotName: slotName,
	}
}

// Append 在一个事务里批量写入事件并推进断点位点。
func (e *EventStore) Append(ctx context.Context, events []Event, commitLSN string) error {
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("开启事务失败: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, ev := range events {
		payload, err := json.Marshal(ev.Columns)
		if err != nil {
			return fmt.Errorf("序列化列值失败: %w", err)
		}
		if _, err := tx.Exec(ctx, `
		INSERT INTO cdc_events (lsn, commit_lsn, operation, schema_name, table_name, payload) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (lsn) DO NOTHING`, ev.LSN, commitLSN, ev.Operation, ev.Schema, ev.Table, payload); err != nil {
			return fmt.Errorf("写入事件失败: %w", err)
		}
	}

	if _, err := tx.Exec(ctx, `
	INSERT INTO cdc_checkpoint (slot_name, confirmed_lsn, updated_at) VALUES ($1, $2, now()) ON CONFLICT (slot_name) DO UPDATE SET confirmed_lsn = EXCLUDED.confirmed_lsn, updated_at = now()`, e.slotName, commitLSN); err != nil {
		return fmt.Errorf("更新断点失败: %w", err)
	}

	return nil
}

// SaveCheckpoint 仅写入断点位点，定死位置
func (s *EventStore) SaveCheckpoint(ctx context.Context, lsn string) error {
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO cdc_checkpoints (slot_name, confirmed_lsn, updated_at)
		VALUES ($1, $2, now())
		ON CONFLICT (slot_name) DO UPDATE
			SET confirmed_lsn = EXCLUDED.confirmed_lsn, updated_at = now()`,
		s.slotName, lsn); err != nil {
		return fmt.Errorf("写入断点失败: %w", err)
	}
	return nil
}

// LoadCheckpoint 读取已落 outbox 的 slot 的断点位点，没有则返回 ok=false。
func (s *EventStore) LoadCheckpoint(ctx context.Context) (string, bool, error) {
	var lsn string
	err := s.pool.QueryRow(ctx,
		"SELECT confirmed_lsn FROM cdc_checkpoints WHERE slot_name = $1", s.slotName).Scan(&lsn)
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

// ListUnprocessed 读取未分发的事件，按 id 升序，最多 limit 条。
func (s *EventStore) ListUnprocessed(ctx context.Context, limit int) ([]StoredEvent, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, lsn, operation, schema_name, table_name, payload
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
			lsn, op, sch, tbl string
			payload           []byte
		)
		if err := rows.Scan(&id, &lsn, &op, &sch, &tbl, &payload); err != nil {
			return nil, fmt.Errorf("扫描事件失败: %w", err)
		}
		var cols []ColumnValue
		if err := json.Unmarshal(payload, &cols); err != nil {
			return nil, fmt.Errorf("解析事件列值失败: %w", err)
		}
		out = append(out, StoredEvent{
			ID: id,
			Event: Event{
				LSN:       lsn,
				Operation: op,
				Schema:    sch,
				Table:     tbl,
				Columns:   cols,
			},
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历事件失败: %w", err)
	}
	return out, nil
}

// MarkProcessed 标记某条事件已分发。
func (s *EventStore) MarkProcessed(ctx context.Context, id int64) error {
	if _, err := s.pool.Exec(ctx,
		`UPDATE cdc_events SET processed_at = NOW() WHERE id = $1`, id); err != nil {
		return fmt.Errorf("标记事件已分发失败: %w", err)
	}
	return nil
}
