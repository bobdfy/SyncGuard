package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/time/rate"
)

// TestRateLimitPerIP 验证限流的两个核心行为：同 IP 超频被拒、不同 IP 各自独立计数。
func TestRateLimitPerIP(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// 速率给很大，让"消耗令牌"完全由 burst 决定，测试不用等补充
	l := NewIPRateLimiter(context.Background(), rate.Limit(1000), 2)
	defer l.Close()

	router := gin.New()
	router.GET("/ping", l.Middleware(), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	do := func(remoteAddr string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/ping", nil)
		// 不设 RemoteAddr 的话 c.ClientIP() 拿到空串，所有请求共用一个桶，测不出分桶效果
		req.RemoteAddr = remoteAddr
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		return w
	}

	const ipA, ipB = "1.1.1.1:1111", "2.2.2.2:2222"

	// burst=2：前两次放行
	for i := 1; i <= 2; i++ {
		if w := do(ipA); w.Code != http.StatusOK {
			t.Fatalf("第 %d 次请求期望 200，实际 %d", i, w.Code)
		}
	}

	// 第三次：桶空了 → 429，且带 Retry-After
	w := do(ipA)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("第 3 次请求期望 429，实际 %d", w.Code)
	}
	if w.Header().Get("Retry-After") == "" {
		t.Error("429 响应缺少 Retry-After 头")
	}

	// 换 IP → 独立桶，第一个请求仍应放行（防止所有 IP 被塞进同一个桶）
	if w := do(ipB); w.Code != http.StatusOK {
		t.Errorf("换 IP 后期望 200，实际 %d", w.Code)
	}
}

// TestSweepRemovesIdleVisitors 验证清理真的能删掉长期不活跃的 visitor（防止 map 无限增长）。
func TestSweepRemovesIdleVisitors(t *testing.T) {
	l := NewIPRateLimiter(context.Background(), rate.Limit(1), 1)
	defer l.Close()

	l.getLimiter("9.9.9.9")
	if _, ok := l.visitors["9.9.9.9"]; !ok {
		t.Fatal("visitor 未创建")
	}

	l.setLastSeen("9.9.9.9", time.Now().Add(-4*time.Minute))
	l.sweep()

	l.mu.Lock()
	_, still := l.visitors["9.9.9.9"]
	l.mu.Unlock()
	if still {
		t.Error("空闲 4 分钟的 visitor 应被清理，实际仍在")
	}
}

// TestSweepKeepsRecentVisitors 防止清理 TTL 被写得过短。
//
// 这是回归测试：如果 idleTTL 小于桶补满时间（burst/r），"清理"就会重置计数，
// 攻击者打满额度后歇几秒就能拿到新桶，限流被绕过。
func TestSweepKeepsRecentVisitors(t *testing.T) {
	l := NewIPRateLimiter(context.Background(), rate.Limit(1), 1)
	defer l.Close()

	l.getLimiter("8.8.8.8")
	l.setLastSeen("8.8.8.8", time.Now().Add(-10*time.Second))

	l.sweep()

	l.mu.Lock()
	_, kept := l.visitors["8.8.8.8"]
	l.mu.Unlock()
	if !kept {
		t.Error("仅空闲 10 秒就被清理了：idleTTL 过短会重置限流计数")
	}
}

// TestRateLimiterClose 验证两种停止方式都能让清理 goroutine 退出（配合优雅退出）。
func TestRateLimiterClose(t *testing.T) {
	// 方式一：显式 Close
	l := NewIPRateLimiter(context.Background(), rate.Limit(1), 1)
	l.Close()
	select {
	case <-l.done:
	default:
		t.Error("Close 之后清理 goroutine 仍未退出")
	}

	// 方式二：父 ctx 取消（server 优雅退出走这条路径）
	ctx, cancel := context.WithCancel(context.Background())
	l2 := NewIPRateLimiter(ctx, rate.Limit(1), 1)
	cancel()
	select {
	case <-l2.done:
	case <-time.After(time.Second):
		t.Error("父 ctx 取消后清理 goroutine 未在一秒内退出")
	}
}

// setLastSeen 把指定 IP 的 lastSeen 拨到给定时间，让测试不必真的等时间过去。
func (l *IPRateLimiter) setLastSeen(ip string, at time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if v, ok := l.visitors[ip]; ok {
		v.lastSeen = at
	}
}
