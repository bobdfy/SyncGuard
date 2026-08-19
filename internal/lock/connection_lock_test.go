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

}

func TestLockMutualExclusion(t *testing.T) {
	l := lock.NewConnectionLock(newTestClient(t))

	_, acquired1, err := l.Lock(context.Background(), 2, 42, 30*time.Second)

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

	_, acquired2, err := l.Lock(t.Context(), 3, 99, 30*time.Second)
	if err != nil {
		t.Fatalf("释放后再次 Lock 出错: %v", err)

	}

	if !acquired2 {
		t.Errorf("释放锁后应该能再次抢到")
	}

}
