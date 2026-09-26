package engine

import "errors"

// ErrNonRetryable 标记「重试也无法成功的永久错误」。
//
// 数据源配置写错（GitHub 的 owner/repo 格式不对、source_type 不支持）
// 就属于这类：重试多少次都一样失败。Worker 应直接进 DLQ 等人工处理，
// 而不是走退避重投白白消耗重试次数和延迟。
var ErrNonRetryable = errors.New("不可重试的永久错误")
