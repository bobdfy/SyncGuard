package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// OutboxStore 维护 outbox 表：业务事务里先落库消息，搬运工随后发送。
type OutboxStore struct {
	db *DB
}

func NewOutboxStore(db *DB) *OutboxStore {
	return &OutboxStore{db: db}
}

// OutboxMessage outbox 表里的一条消息。
type OutboxMessage struct {
	ID      int64
	Payload []byte
}

// Insert 在调用方给定的事务里插入一条待发消息。
// tx 必须由外部传入：保证「业务写库」和「写 outbox」同生共死。
func (s *OutboxStore) Insert(ctx context.Context, tx pgx.Tx, payload []byte) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO outbox(payload) VALUES($1)`, payload)
	if err != nil {
		return fmt.Errorf("OutboxStore Insert: %w", err)
	}
	return nil
}

// ListUnsent 查询未发送（sent_at IS NULL）的消息，按 id 升序，最多 limit 条。
func (s *OutboxStore) ListUnsent(ctx context.Context, limit int) ([]OutboxMessage, error) {
	rows, err := s.db.Pool().Query(ctx,
		`SELECT id, payload FROM outbox WHERE sent_at IS NULL ORDER BY id LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("OutboxStore ListUnsent: %w", err)
	}
	defer rows.Close()

	msgs := make([]OutboxMessage, 0, limit)
	for rows.Next() {
		var m OutboxMessage
		if err := rows.Scan(&m.ID, &m.Payload); err != nil {
			return nil, fmt.Errorf("OutboxStore ListUnsent scan: %w", err)
		}
		msgs = append(msgs, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("OutboxStore ListUnsent rows: %w", err)
	}
	return msgs, nil
}

// MarkSent 标记某条消息已发送。
func (s *OutboxStore) MarkSent(ctx context.Context, id int64) error {
	_, err := s.db.Pool().Exec(ctx,
		`UPDATE outbox SET sent_at = NOW() WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("OutboxStore MarkSent: %w", err)
	}
	return nil
}
