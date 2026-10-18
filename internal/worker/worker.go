package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/bobdfy/syncguard/internal/circuitbreaker"
	"github.com/bobdfy/syncguard/internal/destination"
	"github.com/bobdfy/syncguard/internal/engine"
	"github.com/bobdfy/syncguard/internal/lock"
	"github.com/bobdfy/syncguard/internal/model"
	"github.com/bobdfy/syncguard/internal/mq"
	"github.com/bobdfy/syncguard/internal/redisclient"
	"github.com/bobdfy/syncguard/internal/repository"
	"github.com/bobdfy/syncguard/internal/source"
	"github.com/jackc/pgx/v5"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// lease 是分布式锁的租约时长。超过这个时间没续约，锁自动过期。
	lease = 30 * time.Second

	// renewInterval 是心跳续约间隔，取 lease 的 1/3，
	// 保证任务正常时至少有两次续约机会，不会因为一次续约失败就丢锁。
	renewInterval = lease / 3
)

// Handler 消费 RabbitMQ 消息并执行同步任务。
//
// 把依赖（DB / 锁 / Redis / outbox）收进结构体，Handle 只需一个 msg 参数，
// 比原来 handleJob 平铺传 4 个依赖更清晰，也方便单独写测试。
type Handler struct {
	db       *repository.DB
	connLock *lock.ConnectionLock
	rdb      *redisclient.Client
	outbox   *repository.OutboxStore
}

// New 组装 Handler。
func New(db *repository.DB, connLock *lock.ConnectionLock, rdb *redisclient.Client, outbox *repository.OutboxStore) *Handler {
	return &Handler{db: db, connLock: connLock, rdb: rdb, outbox: outbox}
}

// Handle 处理单条 RabbitMQ 消息的完整生命周期。
// 职责：抢锁 → 心跳续约 → 执行引擎 → 释放锁 → 确认消息。
func (h *Handler) Handle(msg amqp.Delivery) {
	// 1. 反序列化 → JobMessage
	var jobMsg mq.JobMessage
	if err := json.Unmarshal(msg.Body, &jobMsg); err != nil {
		slog.Error("解析 JSON 消息失败", "err", err)
		msg.Nack(false, false)
		return
	}

	// 链路标签：之后这条消息的所有日志都带这三个字段，
	// 排查时按 job_id 一过滤，整条生命周期就串起来了。
	lg := slog.With("job_id", jobMsg.JobID, "task", jobMsg.TaskName, "conn", jobMsg.ConnectionID)
	lg.Info("收到消息")

	// 2. 幂等保护：已完成的任务直接 Ack 跳过
	jobStore := repository.NewJobStore(h.db)
	job, err := jobStore.GetByID(context.Background(), jobMsg.JobID)
	if err != nil {
		// 任务已被删除（不是 DB 抖动）：消息失去意义，直接丢弃。
		// 若走 retryInfra → retryLater 内部又 GetByID 失败 → 原地重排 → 无限自旋刷日志。
		if errors.Is(err, pgx.ErrNoRows) {
			lg.Warn("任务已删除，丢弃消息")
			msg.Ack(false)
			return
		}
		// DB 抖动：退避重投而非进 DLQ（只有业务性失败才进 DLQ）。
		lg.Error("查询任务失败(DB 抖动)，退避重投", "err", err)
		h.retryInfra(msg, jobMsg, "查询任务失败(DB 抖动)", lg)
		return
	}
	if job.Status == model.JobStatusCompleted {
		lg.Info("任务已完成，跳过")
		msg.Ack(false)
		return
	}

	// 3. 抢锁
	credential, acquired, err := h.connLock.Lock(context.Background(), jobMsg.ConnectionID, jobMsg.JobID, lease)
	if err != nil {
		// Redis 抖动：退避重投而非进 DLQ（只有业务性失败才进 DLQ）。
		lg.Error("抢锁失败(Redis 异常)，退避重投", "err", err)
		h.retryInfra(msg, jobMsg, "抢锁失败(Redis 异常)", lg)
		return
	}
	if !acquired {
		h.handleLockBusy(msg, jobMsg, lg)
		return
	}

	// 4. 抢到锁：taskCtx 和 hbCtx 分离
	// taskCtx 传给 eng.Run，Renew 失败时 cancelTask 停任务；
	// hbCtx 只控制心跳 goroutine，正常收尾时 cancelHb 停心跳。
	taskCtx, cancelTask := context.WithCancel(context.Background())
	defer cancelTask()
	hbCtx, cancelHb := context.WithCancel(context.Background())

	var leaseLost atomic.Bool
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		ticker := time.NewTicker(renewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return // 正常收尾：hbCtx 被 cancelHb 取消
			case <-ticker.C:
				ok, err := h.connLock.Renew(hbCtx, jobMsg.ConnectionID, credential, lease)
				if err != nil || !ok {
					if hbCtx.Err() != nil {
						return // hbCtx 已被主动取消，正常收尾，不算丢租约
					}
					// 真正的租约丢失：hbCtx 没被取消，Renew 却失败
					leaseLost.Store(true)
					cancelTask()
					return
				}
			}
		}
	}()

	// 5. 组装引擎并执行（ctx 用 taskCtx，Renew 失败能真正中断任务）
	runErr := h.runSync(taskCtx, jobMsg, lg)

	// 6. 正常收尾：停心跳 → 等心跳退出 → 释放锁
	cancelHb()
	<-renewDone
	h.unlock(jobMsg.ConnectionID, credential, lg)

	// 7. 判定最终结果并确认消息
	h.settle(msg, jobMsg, leaseLost.Load(), runErr, lg)
}

// handleLockBusy 抢锁失败的分流：同一 job 重复投递直接合并；
// 新触发则记 pending 并延迟重投（延迟 = 锁租约，给持锁方留执行窗口）。
func (h *Handler) handleLockBusy(msg amqp.Delivery, jobMsg mq.JobMessage, lg *slog.Logger) {
	owner, found, err := h.connLock.Owner(context.Background(), jobMsg.ConnectionID)
	if err != nil {
		// Redis 抖动：无法判断持锁者，退避重投而非进 DLQ。
		lg.Error("查询锁持有者失败(Redis 异常)，退避重投", "err", err)
		h.retryInfra(msg, jobMsg, "查询锁持有者失败", lg)
		return
	}
	if found && owner == jobMsg.JobID {
		// 同一个 job 的重复投递，合并掉
		lg.Info("重复投递，跳过")
		msg.Ack(false)
		return
	}

	// 新触发：attempt 不递增——这不是失败重试，不消耗重试配额。
	h.requeue(msg, jobMsg, jobMsg.Attempt, lease, "connection 被占用，已延迟重投", lg)
}

// retryInfra 基础设施（DB/Redis）抖动的退避重投：与 settle 的可重试分支一致，
// 到 MaxAttempts 才放弃进 DLQ。优先走 outbox 延迟重投；若 DB 也不可用（outbox 依赖 DB），
// 退化为原地重排 Nack(requeue)，交给 MQ 在基础设施恢复后重投，不丢消息。
func (h *Handler) retryInfra(msg amqp.Delivery, jobMsg mq.JobMessage, reason string, lg *slog.Logger) {
	if jobMsg.Attempt+1 >= mq.MaxAttempts {
		lg.Error("基础设施异常重试到上限，进 DLQ", "reason", reason, "attempt", jobMsg.Attempt)
		msg.Nack(false, false)
		return
	}
	delay := mq.BackoffDelay(jobMsg.Attempt)
	if err := h.retryLater(jobMsg, jobMsg.Attempt+1, delay, reason); err != nil {
		// DB 不可用，outbox 写不进去：原地重排 + 短睡眠节流，避免 tight loop。
		lg.Warn("延迟重投失败，原地重排", "reason", reason, "err", err)
		time.Sleep(time.Second)
		msg.Nack(false, true)
		return
	}
	lg.Info("已延迟重投", "reason", reason, "delay", delay.String(), "attempt", jobMsg.Attempt+1)
	msg.Ack(false)
}

// settle 同步执行结束后的结果分流：Ack / 延迟重投 / 进 DLQ。
func (h *Handler) settle(msg amqp.Delivery, jobMsg mq.JobMessage, leaseLost bool, runErr error, lg *slog.Logger) {
	// 租约丢了，这次同步不可信（可能别的 Worker 已接管），进 DLQ
	if leaseLost {
		lg.Warn("租约丢失，进 DLQ")
		msg.Nack(false, false)
		return
	}

	if runErr == nil {
		lg.Info("执行完成")
		msg.Ack(false)
		return
	}

	// 熔断：数据源冷却中，按冷却时长延迟重投
	if errors.Is(runErr, circuitbreaker.ErrCircuitOpen) {
		delay := circuitbreaker.DefaultConfig().OpenTimeout // 60s，与熔断冷却对齐
		h.requeue(msg, jobMsg, jobMsg.Attempt+1, delay, "熔断中，冷却后重试", lg)
		return
	}

	// 永久错误（数据源配置错）→ 直接 DLQ，重试无意义
	if errors.Is(runErr, engine.ErrNonRetryable) {
		lg.Error("永久失败，进 DLQ", "err", runErr)
		msg.Nack(false, false)
		return
	}

	// 重试次数到上限 → DLQ 放弃
	if jobMsg.Attempt+1 >= mq.MaxAttempts {
		lg.Error("重试次数到上限，进 DLQ", "attempt", jobMsg.Attempt, "err", runErr)
		msg.Nack(false, false)
		return
	}

	// 可重试错误（网络/DB 抖动）→ 指数退避重投
	delay := mq.BackoffDelay(jobMsg.Attempt)
	h.requeue(msg, jobMsg, jobMsg.Attempt+1, delay, "执行失败，退避重试", lg)
}

// requeue 延迟重投的通用收尾：retryLater 成功则 Ack，失败则 Nack 进 DLQ。
func (h *Handler) requeue(msg amqp.Delivery, jobMsg mq.JobMessage, attempt int, delay time.Duration, reason string, lg *slog.Logger) {
	if err := h.retryLater(jobMsg, attempt, delay, reason); err != nil {
		lg.Error("延迟重投失败", "reason", reason, "err", err)
		msg.Nack(false, false)
		return
	}
	lg.Info("已延迟重投", "reason", reason, "delay", delay.String(), "attempt", attempt)
	msg.Ack(false)
}

// retryLater 把任务记为 pending 并写一条延迟 outbox 消息，两者在同一事务。
// Dispatcher 扫到 outbox 记录后按 DelayMs 投递回 RabbitMQ。
// 进度（total_count / cursor）总是以 DB 最新值为准，避免覆盖并发更新。
func (h *Handler) retryLater(jobMsg mq.JobMessage, attempt int, delay time.Duration, reason string) error {
	jobStore := repository.NewJobStore(h.db)

	latest, err := jobStore.GetByID(context.Background(), jobMsg.JobID)
	if err != nil {
		return fmt.Errorf("查询任务 %d 失败: %w", jobMsg.JobID, err)
	}

	payload, err := mq.MarshalOutboxPayload(jobMsg.JobID, jobMsg.TaskName, jobMsg.ConnectionID, attempt, delay.Milliseconds())
	if err != nil {
		return fmt.Errorf("构造 outbox payload 失败: %w", err)
	}

	tx, err := h.db.Begin(context.Background())
	if err != nil {
		return fmt.Errorf("开启事务失败: %w", err)
	}
	defer tx.Rollback(context.Background())

	if err := jobStore.UpdateStatusInTx(context.Background(), tx, jobMsg.JobID, model.JobStatusPending, latest.TotalCount, latest.Cursor, reason); err != nil {
		return fmt.Errorf("记录 pending 失败: %w", err)
	}
	if err := h.outbox.Insert(context.Background(), tx, payload); err != nil {
		return fmt.Errorf("写 outbox 失败: %w", err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		return fmt.Errorf("提交事务失败: %w", err)
	}
	return nil
}

// runSync 组装数据源和目标端，并执行同步引擎。
// ctx 会向下传给数据源、数据库、分页循环，Renew 失败 cancel 后能真正停下。
func (h *Handler) runSync(ctx context.Context, jobMsg mq.JobMessage, lg *slog.Logger) error {
	connectionStore := repository.NewConnectionStore(h.db)

	syncedStore := repository.NewSyncedStore(h.db)

	jobStore := repository.NewJobStore(h.db)
	job, err := jobStore.GetByID(ctx, jobMsg.JobID)
	if err != nil {
		return fmt.Errorf("查询任务失败: %w", err)
	}

	src, err := source.NewSource(ctx, h.db, connectionStore, jobMsg.ConnectionID, job.SyncContent)
	if err != nil {
		return fmt.Errorf("创建数据源失败: %w", err)
	}

	// 轨道分流：数据库源走「原表镜像」，其余走「通用信封」（下方现状）。
	if tableSrc, ok := src.(engine.TableSource); ok {
		return h.runTableSync(ctx, jobMsg, job, tableSrc, connectionStore, jobStore, lg)
	}

	// 熔断器：per-connectionID，状态存 Redis、跨 Worker 共享。
	breaker := circuitbreaker.New(h.rdb.Client(), jobMsg.ConnectionID, circuitbreaker.DefaultConfig())
	src = circuitbreaker.Wrap(src, breaker)
	// 用完关闭数据源，释放其持有的资源（postgres 连接池）。
	defer func() { _ = src.Close() }()

	// 目标接线：目标为空/0 → 内部存储；否则 → 外部目标工厂。
	var dst engine.Destination
	if job.TargetConnectionID == nil || *job.TargetConnectionID == 0 {
		dst = repository.NewInternalDest(syncedStore, job.UserID, job.ConnectionID)
	} else {
		dst, err = destination.NewDestination(ctx, connectionStore, *job.TargetConnectionID, job.SyncContent)
		if err != nil {
			return fmt.Errorf("创建目标端失败: %w", err)
		}
	}
	dst = repository.NewJobDestination(dst, jobStore, jobMsg.JobID)
	// 用完关闭目标端，释放其持有的资源（外部 PG 连接池）。
	defer func() { _ = dst.Close() }()
	eng := engine.New(src, dst, 200)

	lg.Info("开始执行同步")
	return eng.Run(ctx, jobMsg.TaskName)
}

// runTableSync 数据库镜像轨道：读源表结构 → 目标库建同构表 → 列对列写入。
// 与 runSync 剩余的信封路径平行，只处理「数据库源 → 数据库目标」。
func (h *Handler) runTableSync(ctx context.Context, jobMsg mq.JobMessage, job *model.SyncJob, tableSrc engine.TableSource, connectionStore *repository.ConnectionStore, jobStore *repository.JobStore, lg *slog.Logger) error {
	// 决策点：数据库源必须有数据库目标，否则拒绝（镜像数据无处落表）。
	if job.TargetConnectionID == nil || *job.TargetConnectionID == 0 {
		return fmt.Errorf("数据库源必须指定数据库目标(target_connection_id): %w", engine.ErrNonRetryable)
	}

	// 用完关闭源表连接池（WrapTable 包装的是同一底层 Source，这里只关一次）。
	defer func() { _ = tableSrc.Close() }()

	tableDst, err := destination.NewTableDestination(ctx, connectionStore, *job.TargetConnectionID, job.SyncContent)
	if err != nil {
		return fmt.Errorf("创建镜像目标失败: %w", err)
	}
	defer func() { _ = tableDst.Close() }()

	// 熔断器：与信封轨道同款，装饰 TableSource（保护 FetchRows 分页调用）。
	breaker := circuitbreaker.New(h.rdb.Client(), jobMsg.ConnectionID, circuitbreaker.DefaultConfig())
	src := circuitbreaker.WrapTable(tableSrc, breaker)

	progress := repository.NewJobProgress(jobStore, jobMsg.JobID)
	lg.Info("开始镜像同步")
	return engine.RunTable(ctx, src, tableDst, progress, jobMsg.TaskName, 200)
}

// unlock 用独立短超时 ctx 释放锁，不复用已经 cancel 掉的业务 ctx。
// 失败只记录日志，不重跑任务；锁会靠 TTL 自动过期兜底。
func (h *Handler) unlock(connectionID int, credential string, lg *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := h.connLock.Unlock(ctx, connectionID, credential); err != nil {
		lg.Warn("Unlock 失败（靠 TTL 释放）", "err", err)
	}
}
