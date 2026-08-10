package repository

import (
	"context"
	"fmt"

	"github.com/bobdfy/syncguard/internal/model"
)

type UserStore struct {
	db *DB
}

func NewUserStore(db *DB) *UserStore {
	return &UserStore{db: db}
}

func (s *UserStore) Create(ctx context.Context, username string, passwordHash string) error {
	_, err := s.db.Pool().Exec(ctx, `INSERT INTO users( username, password_hash) VALUES ($1, $2)`, username, passwordHash)
	if err != nil {
		return fmt.Errorf("UserStore Create err: %w", err)
	}
	return nil
}

func (s *UserStore) GetByUsername(ctx context.Context, username string) (*model.User, error) {
	var u model.User
	err := s.db.Pool().QueryRow(ctx,
		`SELECT id, username, password_hash, created_at FROM users WHERE username = $1`, username,
	).Scan(&u.ID, &u.UserName, &u.PasswordHash, &u.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("UserStore Get err: %w", err)
	}
	return &u, nil
}
