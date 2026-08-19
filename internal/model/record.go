package model

import (
	"encoding/json"
	"time"
)

// Record 源端返回的通用记录
// 对应 V0 文档中的 JSON 结构:
//
//	{
//	  "id": "record_1001",
//	  "version": 3,
//	  "updated_at": "2026-07-20T10:00:00Z",
//	  "data": {}
//	}
type Record struct {
	ID        string          `json:"id"`
	Version   int             `json:"version"`
	UpdatedAt time.Time       `json:"updated_at"`
	Data      json.RawMessage `json:"data"`
}
