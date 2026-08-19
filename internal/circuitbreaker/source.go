package circuitbreaker

import (
	"context"
	"errors"
	"fmt"

	"github.com/bobdfy/syncguard/internal/engine"
	"github.com/bobdfy/syncguard/internal/model"
)

// ErrCircuitOpen 熔断打开：数据源当前不可用，请求被快速失败。
// Worker 捕获此错误后走「延迟重试 / DLQ」路径，而不是当普通失败处理。
var ErrCircuitOpen = errors.New("熔断器打开：数据源快速失败")

// breakerSource 熔断器装饰器：包一层 Source，在调用前后做「放行判断 + 结果上报」。
// 它自己也实现 engine.Source，所以对 Engine 透明（Engine 完全不知道熔断器的存在）。
type breakerSource struct {
	inner   engine.Source
	breaker *CircuitBreaker
}

// Wrap 用熔断器包装数据源，返回实现了 engine.Source 的装饰器。
// 用法：src = Wrap(src, breaker)，之后把 src 交给 Engine 即可，Engine 无需改动。
func Wrap(src engine.Source, breaker *CircuitBreaker) engine.Source {
	return &breakerSource{inner: src, breaker: breaker}
}

// Fetch 实现 engine.Source 接口。
//
// 流程：
//  1. Allow 判断是否放行；不放行直接返回 ErrCircuitOpen（快速失败，不真调数据源）
//  2. 放行后调用内层 Source.Fetch
//  3. 结果上报：成功 → RecordSuccess；失败 → 先区分是否 context.Canceled
//
// 失败判定：context.Canceled（任务主动取消 / 丢租约）不计入熔断；
// 其余错误（超时 / 网络 / 5xx / 限流 / 404）都计入熔断。
func (b *breakerSource) Fetch(ctx context.Context, cursor string, limit int) ([]model.Record, string, bool, error) {
	// 调 Allow，拿 allowed（是否放行）和 probeToken（探针令牌）
	allowed, probeToken, err := b.breaker.Allow(ctx)

	if err != nil {
		return nil, "", false, fmt.Errorf("熔断器 Allow: %w", err)
	}
	if !allowed {
		// 熔断中：快速失败，不真调数据源
		return nil, "", false, ErrCircuitOpen
	}

	records, nextCursor, hasMore, err := b.inner.Fetch(ctx, cursor, limit)
	if err != nil {
		// 判断是否主动取消：errors.Is 判断 err 是否为 context.Canceled或永久错误
		if errors.Is(err, context.Canceled) || errors.Is(err, engine.ErrNonRetryable) {
			// 主动取消（丢租约）不计失败，直接返回错误，不改熔断状态
			return nil, "", false, err
		}
		// 依赖失败：上报熔断器（RecordFailure）
		_ = b.breaker.RecordFailure(ctx, probeToken)
		return nil, "", false, err
	}

	// 成功：上报熔断器（RecordSuccess）
	_ = b.breaker.RecordSuccess(ctx, probeToken)
	return records, nextCursor, hasMore, nil
}
