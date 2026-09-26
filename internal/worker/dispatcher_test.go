package worker

import (
	"context"
	"testing"
	"time"

	"github.com/bobdfy/syncguard/internal/mq"
	"github.com/bobdfy/syncguard/internal/repository"
)

// ---- fakes ----

type publishCall struct {
	jobID        int
	taskName     string
	connectionID int
	attempt      int
}

type delayedCall struct {
	jobID        int
	taskName     string
	connectionID int
	attempt      int
	delay        time.Duration
}

// fakeProducer 记录调用，可配置错误。
type fakeProducer struct {
	publishCalls []publishCall
	delayedCalls []delayedCall
	publishErr   error
	delayedErr   error
}

func (f *fakeProducer) Publish(ctx context.Context, jobID int, taskName string, connectionID int, attempt int) error {
	f.publishCalls = append(f.publishCalls, publishCall{jobID, taskName, connectionID, attempt})
	return f.publishErr
}

func (f *fakeProducer) PublishDelayed(ctx context.Context, jobID int, taskName string, connectionID int, attempt int, delay time.Duration) error {
	f.delayedCalls = append(f.delayedCalls, delayedCall{jobID, taskName, connectionID, attempt, delay})
	return f.delayedErr
}

// fakeStore 返回预设消息，记录 MarkSent 调用。
type fakeStore struct {
	msgs    []repository.OutboxMessage
	marked  []int64
	listErr error
	markErr error
}

func (f *fakeStore) ListUnsent(ctx context.Context, limit int) ([]repository.OutboxMessage, error) {
	return f.msgs, f.listErr
}

func (f *fakeStore) MarkSent(ctx context.Context, id int64) error {
	f.marked = append(f.marked, id)
	return f.markErr
}

// msg 构造一条 outbox 消息。
func msg(id int64, payload []byte) repository.OutboxMessage {
	return repository.OutboxMessage{ID: id, Payload: payload}
}

// ---- tests ----

// TestProcessOnceImmediate 立即消息：走 Publish，成功后 MarkSent。
func TestProcessOnceImmediate(t *testing.T) {
	payload, err := mq.MarshalOutboxPayload(10, "sync_users", 2, 0, 0)
	if err != nil {
		t.Fatalf("构造 payload 失败: %v", err)
	}
	producer := &fakeProducer{}
	store := &fakeStore{msgs: []repository.OutboxMessage{msg(1, payload)}}

	d := NewDispatcher(producer, store)
	if err := d.processOnce(context.Background()); err != nil {
		t.Fatalf("processOnce 报错: %v", err)
	}

	if len(producer.publishCalls) != 1 {
		t.Fatalf("期望 1 次 Publish，实际 %d 次", len(producer.publishCalls))
	}
	c := producer.publishCalls[0]
	if c.jobID != 10 || c.taskName != "sync_users" || c.connectionID != 2 || c.attempt != 0 {
		t.Errorf("Publish 参数错误: %+v", c)
	}
	if len(producer.delayedCalls) != 0 {
		t.Errorf("立即消息不该走 PublishDelayed")
	}
	if len(store.marked) != 1 || store.marked[0] != 1 {
		t.Errorf("期望 MarkSent(1)，实际 %v", store.marked)
	}
}

// TestProcessOnceDelayed 延迟消息：走 PublishDelayed（毫秒转 Duration），成功后 MarkSent。
func TestProcessOnceDelayed(t *testing.T) {
	payload, err := mq.MarshalOutboxPayload(20, "sync_orders", 3, 2, 5000)
	if err != nil {
		t.Fatalf("构造 payload 失败: %v", err)
	}
	producer := &fakeProducer{}
	store := &fakeStore{msgs: []repository.OutboxMessage{msg(7, payload)}}

	d := NewDispatcher(producer, store)
	if err := d.processOnce(context.Background()); err != nil {
		t.Fatalf("processOnce 报错: %v", err)
	}

	if len(producer.delayedCalls) != 1 {
		t.Fatalf("期望 1 次 PublishDelayed，实际 %d 次", len(producer.delayedCalls))
	}
	dc := producer.delayedCalls[0]
	if dc.jobID != 20 || dc.taskName != "sync_orders" || dc.connectionID != 3 || dc.attempt != 2 {
		t.Errorf("PublishDelayed 参数错误: %+v", dc)
	}
	if dc.delay != 5*time.Second {
		t.Errorf("期望延迟 5s，实际 %v", dc.delay)
	}
	if len(producer.publishCalls) != 0 {
		t.Errorf("延迟消息不该走 Publish")
	}
	if len(store.marked) != 1 || store.marked[0] != 7 {
		t.Errorf("期望 MarkSent(7)，实际 %v", store.marked)
	}
}

// TestProcessOncePublishFail 发送失败：不 MarkSent，留给下一轮重试。
func TestProcessOncePublishFail(t *testing.T) {
	payload, err := mq.MarshalOutboxPayload(30, "sync_users", 1, 0, 0)
	if err != nil {
		t.Fatalf("构造 payload 失败: %v", err)
	}
	producer := &fakeProducer{publishErr: context.DeadlineExceeded}
	store := &fakeStore{msgs: []repository.OutboxMessage{msg(3, payload)}}

	d := NewDispatcher(producer, store)
	if err := d.processOnce(context.Background()); err != nil {
		t.Fatalf("processOnce 报错: %v", err)
	}

	if len(producer.publishCalls) != 1 {
		t.Fatalf("期望尝试 1 次 Publish，实际 %d 次", len(producer.publishCalls))
	}
	if len(store.marked) != 0 {
		t.Errorf("发送失败不应 MarkSent，实际 %v", store.marked)
	}
}

// TestProcessOnceBadPayload 非法 payload：跳过该条、不发送不标记，继续处理下一条。
func TestProcessOnceBadPayload(t *testing.T) {
	good, err := mq.MarshalOutboxPayload(40, "sync_users", 1, 0, 0)
	if err != nil {
		t.Fatalf("构造 payload 失败: %v", err)
	}
	producer := &fakeProducer{}
	store := &fakeStore{
		msgs: []repository.OutboxMessage{
			msg(11, []byte("{not-json")), // 非法
			msg(12, good),                // 合法，应该照常处理
		},
	}

	d := NewDispatcher(producer, store)
	if err := d.processOnce(context.Background()); err != nil {
		t.Fatalf("processOnce 报错: %v", err)
	}

	if len(producer.publishCalls) != 1 {
		t.Fatalf("期望只有合法消息被发送（1 次），实际 %d 次", len(producer.publishCalls))
	}
	if len(store.marked) != 1 || store.marked[0] != 12 {
		t.Errorf("期望只 MarkSent(12)，实际 %v", store.marked)
	}
}

// TestProcessOnceMarkSentFail MarkSent 失败：不 panic、不中断，下轮靠幂等兜底重发。
func TestProcessOnceMarkSentFail(t *testing.T) {
	payload, err := mq.MarshalOutboxPayload(50, "sync_users", 1, 0, 0)
	if err != nil {
		t.Fatalf("构造 payload 失败: %v", err)
	}
	producer := &fakeProducer{}
	store := &fakeStore{
		msgs:    []repository.OutboxMessage{msg(9, payload)},
		markErr: context.DeadlineExceeded,
	}

	d := NewDispatcher(producer, store)
	if err := d.processOnce(context.Background()); err != nil {
		t.Fatalf("processOnce 报错: %v", err)
	}

	// 消息已发出（at-least-once），只是标记失败——不能 panic 就行
	if len(producer.publishCalls) != 1 {
		t.Errorf("期望消息已尝试发送，实际 %d 次", len(producer.publishCalls))
	}
}
