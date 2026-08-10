package repository

import (
	"context"
	"fmt"

	"github.com/bobdfy/syncguard/internal/model"
)

type ConnectionStore struct {
	db *DB
}

func NewConnectionStore(db *DB) *ConnectionStore {
	return &ConnectionStore{db: db}
}

func (s *ConnectionStore) Create(ctx context.Context, userID int, name string, sourceType string, sourceURL string) error {
	_, err := s.db.Pool().Exec(ctx,
		`INSERT INTO connections(user_id, name,source_type, source_url) VALUES($1, $2, $3, $4)`, userID, name, sourceType, sourceURL)
	if err != nil {
		return fmt.Errorf("ConnectionStore err: %w", err)
	}
	return nil
}

// 参数 userID：只查这个用户的，不把别人的数据源也查出来
func (s *ConnectionStore) ListByUser(ctx context.Context, userID int) ([]model.Connection, error) {

	// 1. Query — SELECT 多条，返回一个 rows 游标
	rows, err := s.db.Pool().Query(ctx,
		`SELECT id, user_id, name, source_type, source_url, created_at FROM connections WHERE user_id = $1`, userID,
	)
	if err != nil {
		return nil, fmt.Errorf("%w", err)
	}

	// 2. defer — 不管是正常返回还是中途出错，最后都要关 rows，否则连接泄露
	defer rows.Close()

	// 3. 声明空切片（不是 nil，是 []），后面往里面追加
	var conns []model.Connection

	// 4. for rows.Next() — 一行一行往下走，走到最后一行自动退出
	for rows.Next() {
		var c model.Connection

		// 5. rows.Scan — 把当前行的字段值「扫描」到 c 的字段里
		//    顺序必须跟 SELECT 顺序一致
		if err := rows.Scan(&c.ID, &c.UserID, &c.Name, &c.SourceType, &c.SourceURL, &c.CreatedAt); err != nil {
			return nil, fmt.Errorf("%w", err)
		}

		// 6. append — 这行好了，加到结果列表里
		conns = append(conns, c)
	}

	// 7. 全部行读完了，返回整个列表
	return conns, nil
}

func (s *ConnectionStore) GetByID(ctx context.Context, id int) (*model.Connection, error) {
	var c model.Connection
	err := s.db.Pool().QueryRow(ctx, `SELECT id, user_id, name, source_type, source_url, created_at FROM connections WHERE id = $1`, id).Scan(&c.ID, &c.UserID, &c.Name, &c.SourceType, &c.SourceURL, &c.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("ConnectionStore GetByID err: %w", err)
	}
	return &c, nil
}

// Update 修改数据源 name、source_type、source_url
// WHERE id=$? AND user_id=$? —— 双重条件，防止用户 A 改了用户 B 的数据源
func (s *ConnectionStore) Update(ctx context.Context, id int, userID int, name string, sourceType string, sourceURL string) error {
	_, err := s.db.Pool().Exec(ctx,
		`UPDATE connections SET name=$1, source_type=$2, source_url=$3 WHERE id=$4 AND user_id=$5`,
		name, sourceType, sourceURL, id, userID,
	)
	if err != nil {
		return fmt.Errorf("ConnectionStore Update err: %w", err)
	}
	return nil
}

// Delete 删除数据源
// WHERE id=$? AND user_id=$? —— 防止删别人的数据源
func (s *ConnectionStore) Delete(ctx context.Context, id int, userID int) error {
	_, err := s.db.Pool().Exec(ctx,
		`DELETE FROM connections WHERE id=$1 AND user_id=$2`,
		id, userID,
	)
	if err != nil {
		return fmt.Errorf("ConnectionStore Delete err: %w", err)
	}
	return nil
}
