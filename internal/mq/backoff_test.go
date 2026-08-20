package mq_test

import (
	"testing"
	"time"

	"github.com/bobdfy/syncguard/internal/mq"
)

func TestBackoffDelayMinimum(t *testing.T) {
	if d := mq.BackoffDelay(0); d < 5*time.Second {
		t.Errorf("attempt=0 的延迟应 >= 5s, 实际 %v", d)
	}
}

func TestBackoffDelayMax(t *testing.T) {
	d := mq.BackoffDelay(100)
	if d < 2*time.Minute {
		t.Errorf("大 attempt 应封顶在 2 分钟以上，实际 %v", d)
	}
	if d >= 3*time.Minute {
		t.Errorf("大 attempt 应 < 3min(2min + 抖动)，实际 %v", d)
	}
}

func TestBackoffDelayIncreases(t *testing.T) {
	d0 := mq.BackoffDelay(0)
	d1 := mq.BackoffDelay(1)
	d2 := mq.BackoffDelay(2)

	if !(d0 < d1 && d1 < d2) {
		t.Errorf("延迟应随 attempt 递增，实际 %v, %v, %v", d0, d1, d2)
	}
}
