# SyncGuard 项目长期笔记

## 项目定位

个人学习项目：分布式数据同步系统（Go 1.26 + Gin + pgx + RabbitMQ + Redis）。
Server/Worker 双进程；Transactional Outbox、Redis 分布式锁+心跳续约、三态熔断、
指数退避+DLQ、双轨道（信封/原表镜像）、数据对账、多用户 JWT 隔离。

## 用户目标与偏好（2026-10-09）

- **求职目标：任何 Go 相关的后端/数据工程岗位**。不做算法岗包装，定位 AI Infra / 数据基础设施。
- 偏好"具体可落地的方案"而非泛泛建议：要代码级改法、验收标准、工作量预估。
- 学习风格：认真问原理（为什么用 logging 包 vs slog、为什么传 lg），值得讲透设计取舍。
- 会自己动手改代码然后让我 review —— review 时要逐条指出真 bug、风格问题、误删的注释。

## 改进路线（P0-P3，详见 docs/改进方案-P0到P3.md）

- **P0 已完成**（2026-10-09）：worker.Handle 重构（拆 handleLockBusy/settle/requeue/retryLater，
  240→~95 行）+ 全项目 log → slog 结构化日志（logging 包只做 Init 配置，业务代码直接用 slog）
- P1 部分完成（2026-10-10）：SIGTERM 优雅退出 ✅、config 统一 ✅（用户独立完成）；待做：JWT 24h 过期、API 限流、source_url AES-GCM 加密
- P2 待做：Prometheus 指标、DLQ 管理 API、对账异步化
- P3 待做：MySQL 数据源、定时对账、Kafka 目标端

## 执行顺序（2026-10-09 与用户确认）

1. P0 已完成 → 2. P1（SIGTERM/config/JWT/限流，1.5 天）→ 3. 真 CDC 阶段 1-2（3 天，RAG 管道地基）
→ 4. 检索/embedding/评测最小可用 → 5. 回头补 P2（Prometheus 指标对 CDC lag 是刚需、DLQ 管理）
→ 6. 有余力再做 P1-4 加密、P2-3 对账异步化 → 7. P3（MySQL/Kafka）优先级最低。

理由：SyncGuard 架构已完整，继续加 MySQL/Kafka 是同质化重复劳动，简历边际收益递减；
CDC + RAG 是新增能力维度。且 P1 的优雅退出对 CDC reader 是硬需求（退出要正确释放复制槽）。
CDC 落地位置：同一仓库开 `internal/cdc/` + `cmd/cdcreader/`，复用现有 config/logging/lock/outbox。

详见 `docs/RAG管道-CDC设计.md`。核心决策：
- 必须做**真 CDC**（pgx/pglogrepl 逻辑复制 + LSN 断点），不能沿用现有游标轮询（抓不到 delete）
- 两阶段：CDC Reader 快落事件表（事务内含 LSN），Index Worker 异步切分+embedding —— 复用 outbox 思路
- 向量库选 pgvector 起步（复用现有 PG，回避双写一致性）；Milvus 跳过
- Go 扛全管道，Python 只留 ~200 行（FastAPI embedding/rerank + 评测脚本）
- 重心在 Go 管道工程，检索/重排保持最小可用
