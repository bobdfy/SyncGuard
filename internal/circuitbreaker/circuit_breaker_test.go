package circuitbreaker_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bobdfy/syncguard/internal/circuitbreaker"
	"github.com/redis/go-redis/v9"
)

// 每个测试连一次 Redis，用自定义短冷却配置
func newTestBreaker(t *testing.T, connectionID int) *circuitbreaker.CircuitBreaker {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	// 连不上 Redis 就跳过（而不是报错失败）
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		t.Skipf("Redis 未启动，跳过测试: %v", err)
	}
	// 清理：删除本次测试的熔断状态 key，避免残留影响下次运行
	t.Cleanup(func() {
		client.Del(context.Background(),
			fmt.Sprintf("breaker:connection:%d", connectionID),
			fmt.Sprintf("breaker:connection:%d:probe", connectionID))
		_ = client.Close()
	})
	cfg := circuitbreaker.Config{
		MaxFailures: 5,
		OpenTimeout: 100 * time.Millisecond,
		ProbeTTL:    200 * time.Millisecond,
	}
	return circuitbreaker.New(client, connectionID, cfg)
}

// 测试1：连续失败达到阈值后，Allow 应拒绝
func TestBreakerOpensAfterFailures(t *testing.T) {
	cb := newTestBreaker(t, 1)

	for range 5 {
		if err := cb.RecordFailure(context.Background(), ""); err != nil {
			t.Fatalf("RecordFailure 出错: %v", err)
		}
	}

	allowed, _, err := cb.Allow(context.Background())
	if err != nil {
		t.Fatalf("Allow 出错: %v", err)
	}
	if allowed {
		t.Errorf("失败 5 次后应熔断，实际 还是允许放行")
	}

}

// 测试2：熔断冷却到期后，Allow 应放行并返回探针令牌
func TestBreakerProbeAfterCooldown(t *testing.T) {
	cb := newTestBreaker(t, 2) // 注意：connectionID 用 2，和测试1不冲突

	// 先制造熔断：失败 5 次
	for range 5 {
		if err := cb.RecordFailure(context.Background(), ""); err != nil {
			t.Fatalf("RecordFailure 出错: %v", err)
		}
	}

	// 熔断中：Allow 应该拒绝
	allowed, _, err := cb.Allow(context.Background())
	if err != nil {
		t.Fatalf("Allow 出错: %v", err)
	}
	if allowed {
		t.Fatalf("此时应处于熔断中")
	}

	// 等待超过冷却期（150ms > OpenTimeout 100ms），留余量避免慢机器上贴边界
	time.Sleep(150 * time.Millisecond)

	// 冷却到期后：应放行，且拿到非空探针令牌
	allowed, probeToken, err := cb.Allow(context.Background())
	if err != nil {
		t.Fatalf("Allow 出错: %v", err)
	}
	if !allowed {
		t.Errorf("冷却到期后应放行，实际被拒绝")
	}
	if probeToken == "" {
		t.Errorf("冷却到期后应返回探针令牌，实际为空")
	}
}

// 测试3：探针成功上报后，熔断器回到 closed
func TestBreakerClosesAfterProbeSuccess(t *testing.T) {
	cb := newTestBreaker(t, 3) // connectionID 用 3

	// ① 制造熔断
	for range 5 {
		if err := cb.RecordFailure(context.Background(), ""); err != nil {
			t.Fatalf("RecordFailure 出错: %v", err)
		}
	}

	// ② 等冷却，抢探针（留余量）
	time.Sleep(150 * time.Millisecond)

	_, probeToken, err := cb.Allow(context.Background())
	if err != nil {
		t.Fatalf("Allow 出错: %v", err)
	}
	if probeToken == "" {
		t.Fatalf("应拿到探针令牌")
	}

	// ③ 探针成功上报
	if err := cb.RecordSuccess(context.Background(), probeToken); err != nil {
		t.Fatalf("RecordSuccess 出错: %v", err)
	}

	// ④ 断言回到 closed
	allowed, token, err := cb.Allow(context.Background())
	if err != nil {
		t.Fatalf("Allow 出错: %v", err)
	}
	if !allowed {
		t.Errorf("探针成功后应回 close, 实际被拒绝")
	}
	if token != "" {
		t.Errorf("回 closed 后不应再返回探针令牌，实际 %q", token)
	}
}
