package circuitbreaker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// allowScript 判断放行/抢占探针，返回 {allowed, probeToken}。
// KEYS[1]=hashKey  KEYS[2]=probeKey
// ARGV[1]=probeToken  ARGV[2]=probeTTL(ms)  ARGV[3]=now(ms)
var allowScript = redis.NewScript(`
local state = redis.call("HGET", KEYS[1], "state")
if state == "open" then
    local open_until = tonumber(redis.call("HGET", KEYS[1], "open_until") or "0")
    if tonumber(ARGV[3]) < open_until then
        return {0, ""}
    end
    local ok = redis.call("SET", KEYS[2], ARGV[1], "NX", "PX", ARGV[2])
    if ok then
        redis.call("HSET", KEYS[1], "state", "half_open")
        return {1, ARGV[1]}
    else
        return {0, ""}
    end
end
if state == "half_open" then
    if redis.call("EXISTS", KEYS[2]) == 0 then
        local ok = redis.call("SET", KEYS[2], ARGV[1], "NX", "PX", ARGV[2])
        if ok then
            return {1, ARGV[1]}
        end
    end
    return {0, ""}
end
return {1, ""}
`)

// recordSuccessScript上报成功。探针成功 → 关回closed并清零失败计数。
// KEYS[1]=hashKey  KEYS[2]=probeKey  ARGV[1]=probeToken
var recordSuccessScript = redis.NewScript(`
if ARGV[1] ~= "" then
    if redis.call("GET", KEYS[2]) ~= ARGV[1] then
        return 0
    end
    redis.call("DEL", KEYS[2])
end
redis.call("HSET", KEYS[1], "state", "closed", "failures", 0)
redis.call("HDEL", KEYS[1], "open_until")
return 1
`)

// recordFailureScript 上报失败。探针失败 → 直接 open；普通失败 → 累计计数，达阈值 open。
//
// KEYS[1]=hashKey  KEYS[2]=probeKey
// ARGV[1]=probeToken  ARGV[2]=now(ms)  ARGV[3]=openTimeout(ms)  ARGV[4]=maxFailures
var recordFailureScript = redis.NewScript(`
if ARGV[1] ~= "" then
    if redis.call("GET", KEYS[2]) ~= ARGV[1] then
        return 0
    end
    redis.call("DEL", KEYS[2])
    redis.call("HSET", KEYS[1], "state", "open", "open_until", tonumber(ARGV[2]) + tonumber(ARGV[3]))
    return 1
end
local failures = redis.call("HINCRBY", KEYS[1], "failures", 1)
if failures >= tonumber(ARGV[4]) then
    redis.call("HSET", KEYS[1], "state", "open", "open_until", tonumber(ARGV[2]) + tonumber(ARGV[3]))
end
return 1
`)

// CircuitBreaker 熔断器：防止某数据源持续失败时，Worker 无限重试把系统拖垮。
// Redis 存两个 key：
//
//	hash  key = breaker:connection:{id}  字段 state / failures / open_until
//	probe key = breaker:connection:{id}:probe  探针令牌（SET NX + TTL）
type CircuitBreaker struct {
	client      *redis.Client
	maxFailures int
	openTimeout time.Duration
	probeTTL    time.Duration
	hashKey     string
	probeKey    string
}

// Config 熔断器配置。
type Config struct {
	MaxFailures int           // 连续失败多少次熔断
	OpenTimeout time.Duration // 冷却时长
	ProbeTTL    time.Duration // 探针令牌 TTL
}

// DefaultConfig 默认：失败 5 次熔断、冷却 60s、探针 TTL 35s。
// 探针 TTL(35s) 故意 > HTTP 超时(30s)，保证探针请求在 TTL 内完成上报。
func DefaultConfig() Config {
	return Config{
		MaxFailures: 5,
		OpenTimeout: 60 * time.Second,
		ProbeTTL:    35 * time.Second,
	}
}

// New 创建熔断器。breaker 只是 client+key+config 的载体；
// 状态本身在 Redis，所以每个 Worker 各建一个实例也不冲突（天然共享）。
func New(client *redis.Client, connectionID int, cfg Config) *CircuitBreaker {
	return &CircuitBreaker{
		client:      client,
		maxFailures: cfg.MaxFailures,
		openTimeout: cfg.OpenTimeout,
		probeTTL:    cfg.ProbeTTL,
		hashKey:     fmt.Sprintf("breaker:connection:%d", connectionID),
		probeKey:    fmt.Sprintf("breaker:connection:%d:probe", connectionID),
	}
}

// Allow 判断本次请求能否放行。
// 即使 closed 也要先生成 token，因为 open→half_open 抢占探针时要用。
func (cb *CircuitBreaker) Allow(ctx context.Context) (allowed bool, probeToken string, err error) {
	token, err := newProbeToken() //随机生成一个探针令牌
	if err != nil {
		return false, "", fmt.Errorf("Allow 生成探针令牌: %w", err)
	}

	res, err := allowScript.Run(ctx, cb.client, []string{cb.hashKey, cb.probeKey}, token, cb.probeTTL.Milliseconds(), time.Now().UnixMilli()).Result()

	if err != nil {
		return false, "", fmt.Errorf("allow script: %w", err)
	}

	arr, ok := res.([]any)
	if !ok || len(arr) != 2 {
		return false, "", fmt.Errorf("Allow script 返回值格式错误: %v", res)
	}
	allowed = arr[0].(int64) == 1
	probeToken = arr[1].(string)
	return allowed, probeToken, nil
}

// RecordSuccess 上报成功。probeToken 非空说明是探针请求，脚本会先校验令牌。
func (cb *CircuitBreaker) RecordSuccess(ctx context.Context, probeToken string) error {
	_, err := recordSuccessScript.Run(ctx, cb.client, []string{cb.hashKey, cb.probeKey}, probeToken).Int64()
	if err != nil {
		return fmt.Errorf("recordsuccess script: %w", err)
	}

	return nil
}

// RecordFailure 上报失败。probeToken 非空走探针分支（校验令牌后直接 open）；
// 空串走普通分支（HINCRBY 累计失败次数，达阈值熔断）。
func (cb *CircuitBreaker) RecordFailure(ctx context.Context, probeToken string) error {
	_, err := recordFailureScript.Run(ctx, cb.client, []string{cb.hashKey, cb.probeKey}, probeToken, time.Now().UnixMilli(), cb.openTimeout.Milliseconds(), cb.maxFailures).Int64()
	if err != nil {
		return fmt.Errorf("recordfailure script: %w", err)
	}

	return nil
}

// newProbeToken 生成 32 位十六进制随机串，作为探针令牌（同阶段2 锁的 newToken）。
func newProbeToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("生成探针令牌: %w", err)
	}
	return hex.EncodeToString(b), nil
}
