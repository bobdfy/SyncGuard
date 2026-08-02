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
	// TODO: ID 字符串，如 "record_0001"
	ID string
	// TODO: Version 整数，每次更新 +1
	Version int
	// TODO: UpdatedAt 时间字符串（RFC3339）
	UpdatedAt time.Time
	// TODO: Data 原始 JSON 载荷（json.RawMessage）
	Data json.RawMessage
}

// RecordPage 一页查询结果
type RecordPage struct {
	// TODO: Records 本页记录切片
	Records []Record
	// TODO: NextCursor 下一页游标，空字符串表示没有下一页
	NextCursor string
	// TODO: HasMore 是否还有更多数据
	HasMore bool
}
