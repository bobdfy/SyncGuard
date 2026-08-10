package model

import "time"

//User 用户信息
type User struct {
	ID           int64     `json:"id"`
	UserName     string    `json:"user_name"`
	PasswordHash string    `json:"password_hash"`
	CreatedAt    time.Time `json:"created_at"`
}
