package middleware

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/time/rate"
)

// idleTTL 是 visitor 被判定为"不活跃"的时长，超过它就会被清理 goroutine 删除。
//
// 这个值有硬下限：必须 >= 桶补满所需时间（burst / r）。
// 否则"清理"会连带把计数也重置——攻击者打满额度后歇一会儿就能拿到一个满桶，限流形同虚设。
// 登录限流是 5 次/分钟（补满 60 秒），取 3 分钟留足余量。
const idleTTL = 3 * time.Minute

// visitor 记录单个 IP 的令牌桶和最后访问时间（供清理判断）。
type visitor struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// IPRateLimiter 按 IP 分桶的限流器。
type IPRateLimiter struct {
	mu       sync.Mutex
	visitors map[string]*visitor
	r        rate.Limit // 每秒产生的令牌数
	burst    int        // 桶容量（允许的瞬间突发）

	cancel context.CancelFunc // 停止后台清理 goroutine
	done   chan struct{}      // 清理 goroutine 已退出的信号
}

// NewIPRateLimiter 创建限流器，并启动后台清理 goroutine。
// ctx 决定清理 goroutine 的生命周期：根 ctx 取消时它随之退出；
func NewIPRateLimiter(ctx context.Context, r rate.Limit, burst int) *IPRateLimiter {
	ctx, cancel := context.WithCancel(ctx)
	l := &IPRateLimiter{
		visitors: make(map[string]*visitor),
		r:        r,
		burst:    burst,
		cancel:   cancel,
		done:     make(chan struct{}),
	}
	go l.cleanupLoop(ctx)
	return l
}

// Close 停止后台清理 goroutine 并等待它真正退出。
func (l *IPRateLimiter) Close() {
	l.cancel()
	<-l.done
}

// cleanupLoop 定期清理长时间不活跃的 visitor。
// 不清理的话 map 会按访问过的 IP 数无限增长，限流器自己就成了内存漏洞。
func (l *IPRateLimiter) cleanupLoop(ctx context.Context) {
	defer close(l.done)

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return // 优雅退出：根 ctx 被取消，或 Close() 被调用
		case <-ticker.C:
			l.sweep()
		}
	}
}

// sweep 删除空闲超过 idleTTL 的 visitor。
func (l *IPRateLimiter) sweep() {
	l.mu.Lock()
	defer l.mu.Unlock()

	for ip, v := range l.visitors {
		if time.Since(v.lastSeen) > idleTTL {
			delete(l.visitors, ip)
		}
	}
}

// getLimiter 取指定 IP 的令牌桶，没有就按统一参数新建一个。
func (l *IPRateLimiter) getLimiter(ip string) *rate.Limiter {
	l.mu.Lock()
	defer l.mu.Unlock()

	v, ok := l.visitors[ip]
	if !ok {
		lm := rate.NewLimiter(l.r, l.burst)
		l.visitors[ip] = &visitor{limiter: lm, lastSeen: time.Now()}
		return lm
	}
	v.lastSeen = time.Now()
	return v.limiter
}

// Middleware 返回 gin 中间件：取不到令牌的请求直接 429。
func (l *IPRateLimiter) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !l.getLimiter(c.ClientIP()).Allow() {
			// Retry-After 告诉客户端"至少多久后才可能成功"：补一个令牌需要 1/r 秒
			if l.r > 0 {
				wait := time.Duration(float64(time.Second) / float64(l.r))
				c.Header("Retry-After", strconv.Itoa(int(wait.Seconds())))
			}
			c.JSON(http.StatusTooManyRequests, gin.H{"error": "请求过于频繁，请稍后再试"})
			c.Abort()
			return
		}
		c.Next()
	}
}
