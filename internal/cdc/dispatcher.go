package cdc

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"
)

// eventPublisher 分发目标的最小接口
type eventPublisher interface {
	PublishRawConfirmed(ctx context.Context, body []byte) error
}

// eventStore 读取/标记 cdc_events 的最小接口
type eventStore interface {
	ListUnprocessed(ctx context.Context, limit int) ([]StoredEvent, error)
	MarkProcessed(ctx context.Context, id int64) error
}

// 轮询 cdc_events 未分发事件，原样投到 RabbitMQ 的 CDC 队列。
type Dispatcher struct {
	producer eventPublisher
	store    eventStore
}

// NewDispatcher 组装搬运工。
func NewDispatcher(producer eventPublisher, store eventStore) *Dispatcher {
	return &Dispatcher{producer: producer, store: store}
}

// Start 启动搬运循环：每 1 秒扫一次未分发事件；ctx 取消时退出。
func (d *Dispatcher) Start(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			slog.Info("CDC Dispatcher 已停止")
			return
		case <-ticker.C:
			if err := d.processOnce(ctx); err != nil {
				slog.Error("CDC Dispatcher 扫描分发失败", "err", err)
			}
		}
	}
}

// processOnce 扫一轮：查未分发 → 逐条原样发到 MQ → 成功标 processed_at
func (d *Dispatcher) processOnce(ctx context.Context) error {
	events, err := d.store.ListUnprocessed(ctx, 20)
	if err != nil {
		return err
	}

	for _, ev := range events {
		body, err := json.Marshal(ev.Event)
		if err != nil {
			slog.Error("CDC Dispatcher 序列化事件失败，跳过", "event_id", ev.ID, "err", err)
			continue
		}

		if err := d.producer.PublishRawConfirmed(ctx, body); err != nil {
			slog.Warn("CDC Dispatcher 发送事件失败，下轮重试", "event_id", ev.ID, "err", err)
			continue
		}
		if err := d.store.MarkProcessed(ctx, ev.ID); err != nil {
			slog.Warn("CDC Dispatcher 标记已分发失败", "event_id", ev.ID, "err", err)
			continue
		}
		slog.Info("已分发 CDC 事件", "event_id", ev.ID, "operation", ev.Event.Operation, "table", ev.Event.Schema+"."+ev.Event.Table)
	}
	return nil
}
