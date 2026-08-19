package lock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// ConnectionLock 连接级分布式锁。
//
// 用 Redis 实现，协调多个 Worker 进程：同时同步同一个数据源时，
// 只有一个能抢到锁，其余跳过或延迟重试。
//
// 租约 = 带过期时间的锁，完整机制分三步：
//  1. Lock   抢锁（SET NX EX），value 存一个本次唯一的 credential，并返回
//  2. Renew  任务执行中续约（PEXPIRE），校验 credential 防止续错锁
//  3. Unlock 任务结束释放（DEL），校验 credential 防止删错锁
//
// credential 格式为 "{jobID}:{随机token}"：
//   - jobID 部分：让锁能识别「当前是谁在持锁」。抢锁失败时靠 Owner()
//     读出 jobID，判断是同一条任务重复投递，还是新的同步触发。
//   - token 部分：每次 Lock 都随机生成，保证「旧任务的凭证」永远
//     删不掉「新任务的锁」。
type ConnectionLock struct {
	client *redis.Client
}

// NewConnectionLock 创建锁。
func NewConnectionLock(client *redis.Client) *ConnectionLock {
	return &ConnectionLock{client: client}
}

// Lock 抢锁，成功返回本次唯一的 credential，acquired=false 表示已被其他 Worker 持有。
//
// 参数：
//
//	connectionID — 要锁的数据源连接 ID
//	jobID        — 本次同步任务的 ID，会被编码进 credential
//	lease        — 租约时长
//
// 返回：
//
//	credential — 抢到锁时的完整凭证（jobID:token），Renew/Unlock 都要用它
//	acquired   — true=抢到 / false=已被占用
func (l *ConnectionLock) Lock(ctx context.Context, connectionID, jobID int, lease time.Duration) (credential string, acquired bool, err error) {
	key := lockKey(connectionID)
	token, err := newToken()
	if err != nil {
		return "", false, fmt.Errorf("Lock 生成 token: %w", err)
	}
	credential = fmt.Sprintf("%d:%s", jobID, token)

	ok, err := l.client.SetNX(ctx, key, credential, lease).Result()
	if err != nil {
		return "", false, fmt.Errorf("Lock SetNX: %w", err)
	}
	if !ok {
		return "", false, nil
	}
	return credential, true, nil
}

// Owner 返回当前持锁者的 jobID，供抢锁失败时判断「重复投递还是新触发」。
//
// 返回：
//
//	jobID — 当前持锁者的任务 ID
//	found — false 表示锁 key 不存在（锁刚好过期/释放了）
//
// 调用方必须严格区分三种情况，不要混在一起处理：
//
//	err != nil      → Redis 查询失败，无法判断，应保守处理（如进 DLQ）
//	found == false  → 当前没有持锁者（竞态：锁刚被释放）
//	found == true   → 锁被 jobID 持有
func (l *ConnectionLock) Owner(ctx context.Context, connectionID int) (jobID int, found bool, err error) {
	key := lockKey(connectionID)
	val, err := l.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return 0, false, nil // 锁不存在
	}
	if err != nil {
		return 0, false, fmt.Errorf("Owner GET: %w", err)
	}
	jobID, err = parseCredentialJobID(val)
	if err != nil {
		return 0, false, fmt.Errorf("Owner 解析 credential: %w", err)
	}
	return jobID, true, nil
}

// Renew 续约，延长锁的过期时间（心跳）。校验 credential，防止续错别人的锁。
func (l *ConnectionLock) Renew(ctx context.Context, connectionID int, credential string, lease time.Duration) (bool, error) {
	key := lockKey(connectionID)
	// lease.Milliseconds() 返回毫秒，配合 Lua 里的 PEXPIRE 保证单位一致
	result, err := renewScript.Run(ctx, l.client, []string{key}, credential, lease.Milliseconds()).Int64()
	if err != nil {
		return false, fmt.Errorf("Renew script: %w", err)
	}
	return result == 1, nil
}

// Unlock 释放锁。校验 credential，只有锁还是自己的才删，防止删错别人的锁。
func (l *ConnectionLock) Unlock(ctx context.Context, connectionID int, credential string) error {
	key := lockKey(connectionID)
	_, err := unlockScript.Run(ctx, l.client, []string{key}, credential).Int64()
	if err != nil {
		return fmt.Errorf("Unlock script: %w", err)
	}
	return nil
}

// renewScript 校验 credential 再续约（PEXPIRE 毫秒），保证「读 → 判断 → 续约」原子。
var renewScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
    return 0
end
`)

// unlockScript 校验 credential 再删除，保证「读 → 判断 → 删」原子。
var unlockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
`)

// lockKey 生成锁的 key。
func lockKey(connectionID int) string {
	return fmt.Sprintf("lock:connection:%d", connectionID)
}

// newToken 生成 32 位十六进制随机串，作为 credential 的唯一部分。
func newToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("生成随机 token: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// parseCredentialJobID 从 "{jobID}:{token}" 解析出 jobID 部分。
func parseCredentialJobID(credential string) (int, error) {
	idx := strings.Index(credential, ":")
	if idx < 0 {
		return 0, fmt.Errorf("credential 格式错误: %q", credential)
	}
	jobID, err := strconv.Atoi(credential[:idx])
	if err != nil {
		return 0, fmt.Errorf("credential 的 jobID 非法: %q", credential)
	}
	return jobID, nil
}
