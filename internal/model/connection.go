package model

import "time"

type Connection struct {
	ID         int       `json:"id"`
	UserID     int       `json:"user_id"`
	Name       string    `json:"name"`
	SourceType string    `json:"source_type"`
	SourceURL  string    `json:"source_url"`
	CreatedAt  time.Time `json:"created_at"`
}
