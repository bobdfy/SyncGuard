package cdc

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/bobdfy/syncguard/internal/mq"
)

// MQHandler 把解析的变更事件序列化投递到 RabbitMQ 的 CDC 队列
type MQHandler struct {
	producer *mq.Producer
}

func NewMQHanlder(producer *mq.Producer) *MQHandler {
	return &MQHandler{producer: producer}
}

func (h *MQHandler) Handle(ctx context.Context, ev Event) error {
	body, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("序列化 CDC 事件失败: %w", err)
	}
	return h.producer.PublishRaw(ctx, body)
}
