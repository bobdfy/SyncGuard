package model

// Diff 对账差异结果。
//
// 对账就是比较源端（GitHub Issues API 等）和目标端（synced_records 表），
// 找出不一致的记录。
//
// 四种差异类型：
//
//	missing_in_target  — 源端有，目标端缺失（同步漏了）
//	extra_in_target    — 目标端有，源端没有（源端删了，或同步了不该同步的）
//	version_mismatch   — 版本号不一样（目标端过时了）
//	content_mismatch   — 版本号一样但内容不同（目标端数据被篡改或 Hash 碰撞）
type Diff struct {
	SourceID      string `json:"source_id"`       // 源端记录 ID，如 "issue_42"
	DiffType      string `json:"diff_type"`       // missing_in_target / extra_in_target / version_mismatch / content_mismatch
	SourceVersion int    `json:"source_version"`  // 源端当前版本号
	TargetVersion int    `json:"target_version"`  // 目标端版本号（缺失时为 0）
	SourceHash    string `json:"source_hash"`     // 源端数据 Hash
	TargetHash    string `json:"target_hash"`     // 目标端数据 Hash
}