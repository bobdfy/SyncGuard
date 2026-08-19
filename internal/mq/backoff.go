package mq

import (
	"math/rand"
	"time"
)

const (
	// backoffBase 首次重试的基准延迟。
	backoffBase = 5 * time.Second

	// backoffMax 退避封顶，重试间隔最长不超过这个值。
	backoffMax = 2 * time.Minute

	// MaxAttempts 最大投递次数。attempt 从 0（首次）计，
	// 达到 MaxAttempts 后不再退避重投，直接进 DLQ 放弃。
	MaxAttempts = 5
)

// BackoffDelay 计算第 attempt 次重试的延迟。
//
// 公式：base × 2^attempt，封顶 backoffMax，再加随机抖动。
//
//	attempt=0 → 5s，1 → 10s，2 → 20s，3 → 40s，4 → 80s（封顶前）
//
// 抖动取 [0, d/2) 的随机值：同一批失败的任务重试时间被随机错开，
// 避免它们在同一时刻同时重试形成「重试风暴」。
func BackoffDelay(attempt int) time.Duration {
	d := backoffBase
	for i := 0; i < attempt && d < backoffMax; i++ {
		d *= 2
		if d > backoffMax {
			d = backoffMax
		}
	}
	jitter := time.Duration(rand.Int63n(int64(d / 2)))
	return d + jitter
}
