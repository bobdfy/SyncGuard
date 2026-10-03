package circuitbreaker

import (
	"context"
	"errors"
	"fmt"

	"github.com/bobdfy/syncguard/internal/engine"
	"github.com/bobdfy/syncguard/internal/model"
)

// 编译期检查：breakerTableSource 实现了 engine.TableSource 接口
var _ engine.TableSource = (*breakerTableSource)(nil)

// breakerTableSource 熔断器装饰器（镜像轨道版）：包一层 TableSource，
// 对 FetchRows 做与 Fetch 相同的「放行判断 + 结果上报」。TableSchema 是一
// 次性元数据读取，失败直接返回，不参与熔断（每页拉取失败才是熔断对象）。
type breakerTableSource struct {
	inner   engine.TableSource
	breaker *CircuitBreaker
}

// WrapTable 用熔断器包装镜像数据源，返回实现了 engine.TableSource 的装饰器。
func WrapTable(src engine.TableSource, breaker *CircuitBreaker) engine.TableSource {
	return &breakerTableSource{inner: src, breaker: breaker}
}

// TableSchema 透传到底层，不参与熔断。
func (b *breakerTableSource) TableSchema(ctx context.Context) (model.TableSchema, error) {
	return b.inner.TableSchema(ctx)
}

// FetchRows 与 Fetch 同款熔断逻辑：Allow → 调内层 → 上报成功/失败。
func (b *breakerTableSource) FetchRows(ctx context.Context, cursor string, limit int) ([]model.RawRow, string, bool, error) {
	allowed, probeToken, err := b.breaker.Allow(ctx)
	if err != nil {
		return nil, "", false, fmt.Errorf("熔断器 Allow: %w", err)
	}
	if !allowed {
		return nil, "", false, ErrCircuitOpen
	}

	rows, nextCursor, hasMore, err := b.inner.FetchRows(ctx, cursor, limit)
	if err != nil {
		// 主动取消 / 永久错误不计入熔断，其余依赖失败上报熔断器。
		if errors.Is(err, context.Canceled) || errors.Is(err, engine.ErrNonRetryable) {
			return nil, "", false, err
		}
		_ = b.breaker.RecordFailure(ctx, probeToken)
		return nil, "", false, err
	}

	_ = b.breaker.RecordSuccess(ctx, probeToken)
	return rows, nextCursor, hasMore, nil
}

// Close 透传到底层，释放其资源（如 postgres 连接池）。
func (b *breakerTableSource) Close() error {
	return b.inner.Close()
}
