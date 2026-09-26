package mq

import (
	"encoding/json"
	"testing"
)

// TestMarshalOutboxPayload 验证 outbox payload 序列化往返字段一致。
func TestMarshalOutboxPayload(t *testing.T) {
	tests := []struct {
		name         string
		jobID        int
		taskName     string
		connectionID int
		attempt      int
		delayMs      int64
	}{
		{name: "立即发送", jobID: 1, taskName: "sync_users", connectionID: 2, attempt: 0, delayMs: 0},
		{name: "延迟发送（退避重投）", jobID: 3, taskName: "sync_orders", connectionID: 4, attempt: 2, delayMs: 5000},
		{name: "负值边界", jobID: 0, taskName: "", connectionID: 0, attempt: 0, delayMs: -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, err := MarshalOutboxPayload(tt.jobID, tt.taskName, tt.connectionID, tt.attempt, tt.delayMs)
			if err != nil {
				t.Fatalf("MarshalOutboxPayload 报错: %v", err)
			}

			// JSON 里必须出现 delay_ms 键（Dispatcher 靠它决定走哪条发送路径）
			var raw map[string]json.RawMessage
			if err := json.Unmarshal(body, &raw); err != nil {
				t.Fatalf("反序列化为 map 失败: %v", err)
			}
			if _, ok := raw["delay_ms"]; !ok {
				t.Errorf("JSON 缺少 delay_ms 键: %s", body)
			}

			var p OutboxPayload
			if err := json.Unmarshal(body, &p); err != nil {
				t.Fatalf("反序列化失败: %v", err)
			}
			if p.JobID != tt.jobID || p.TaskName != tt.taskName ||
				p.ConnectionID != tt.connectionID || p.Attempt != tt.attempt || p.DelayMs != tt.delayMs {
				t.Errorf("往返后字段不一致: 期望 %+v，实际 %+v", tt, p)
			}
		})
	}
}
