package lock_test

import (
	"context"
	"testing"
	"time"

	"github.com/bobdfy/syncguard/internal/lock"
	"github.com/redis/go-redis/v9"
)

func newTestClient(t *testing.T) *redis.Client {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	// 连不上 Redis 就跳过（而不是报错失败），方便无 Redis 环境跑其他测试
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		t.Skipf("Redis 未启动，跳过测试: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

func TestLockAcquire(t *testing.T) {
	l := lock.NewConnectionLock(newTestClient(t))

	credential, acquired, err := l.Lock(context.Background(), 1, 42, 30*time.Second)

	if err != nil {
		t.Fatalf("Lock 出错: %v", err)
	}

	if !acquired {
		t.Errorf("第一次抢锁应该成功返回ture, 实际为: %v", acquired)
	}

	if credential == "" {
		t.Errorf("credential 不应该为空")
	}

	// 清理：释放测试抢到的锁，避免 key 残留影响下次运行
	t.Cleanup(func() {
		_ = l.Unlock(context.Background(), 1, credential)
	})
}

func TestLockMutualExclusion(t *testing.T) {
	l := lock.NewConnectionLock(newTestClient(t))

	credential1, acquired1, err := l.Lock(context.Background(), 2, 42, 30*time.Second)

	if err != nil {
		t.Fatalf("第一次 Lock 出错: %v", err)
	}

	_, acquired2, err := l.Lock(context.Background(), 2, 99, 30*time.Second)

	if !acquired1 {
		t.Errorf("第一次抢锁应该成功")
	}

	if acquired2 {
		t.Errorf("同一 connectionID 第二次抢锁应该失败")
	}

	// 清理：释放第一个锁，避免 key 残留
	t.Cleanup(func() {
		_ = l.Unlock(context.Background(), 2, credential1)
	})
}

func TestLockReleaseAllowReacquire(t *testing.T) {
	l := lock.NewConnectionLock(newTestClient(t))

	credential, acquired1, err := l.Lock(context.Background(), 3, 42, 30*time.Second)
	if err != nil {
		t.Fatalf("第一次抢锁失败: acquired=%v err=%v", acquired1, err)
	}

	if err := l.Unlock(context.Background(), 3, credential); err != nil {
		t.Fatalf("Unlock 失败: %v", err)
	}

	credential2, acquired2, err := l.Lock(t.Context(), 3, 99, 30*time.Second)
	if err != nil {
		t.Fatalf("释放后再次 Lock 出错: %v", err)
	}

	if !acquired2 {
		t.Errorf("释放锁后应该能再次抢到")
	}

	// 清理：释放第二个锁，避免 key 残留
	t.Cleanup(func() {
		_ = l.Unlock(context.Background(), 3, credential2)
	})
}
