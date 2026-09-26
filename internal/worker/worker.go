package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
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
//
// 职责：抢锁 → 心跳续约 → 执行引擎 → 释放锁 → 确认消息。
func (h *Handler) Handle(msg amqp.Delivery) {
	// 1. 反序列化 → JobMessage
	var jobMsg mq.JobMessage
	if err := json.Unmarshal(msg.Body, &jobMsg); err != nil {
		log.Printf("[Worker] 解析 JSON 消息失败: %v", err)
		msg.Nack(false, false)
		return
	}
	log.Printf("[Worker] 收到消息 job_id=%d task_name=%s", jobMsg.JobID, jobMsg.TaskName)

	jobStore := repository.NewJobStore(h.db)

	// 2. 幂等保护：已完成的任务直接 Ack 跳过
	job, err := jobStore.GetByID(context.Background(), jobMsg.JobID)
	if err != nil {
		log.Printf("[Worker] 查询任务 %d 失败: %v", jobMsg.JobID, err)
		msg.Nack(false, false)
		return
	}
	if job.Status == model.JobStatusCompleted {
		log.Printf("[Worker] 任务已执行，跳过")
		msg.Ack(false)
		return
	}

	// 3. 抢锁
	credential, acquired, err := h.connLock.Lock(context.Background(), jobMsg.ConnectionID, jobMsg.JobID, lease)
	if err != nil {
		// Redis/锁系统异常，任务无法执行，进 DLQ 等后续重试
		log.Printf("[Worker] 抢锁失败(Redis 异常): %v", err)
		msg.Nack(false, false)
		return
	}
	if !acquired {
		// 抢不到锁：判断是「同一 job 重复投递」还是「新同步触发」
		owner, found, err := h.connLock.Owner(context.Background(), jobMsg.ConnectionID)
		if err != nil {
			// Redis 查询失败，无法判断，保守进 DLQ
			log.Printf("[Worker] 查询锁持有者失败: %v", err)
			msg.Nack(false, false)
			return
		}
		if found && owner == jobMsg.JobID {
			// 同一个 job 的重复投递，合并掉
			log.Printf("[Worker] job_id=%d 重复投递，跳过", jobMsg.JobID)
			msg.Ack(false)
			return
		}

		// 新触发（found=false 或 owner != jobID）：记录 pending + 写 outbox 延迟重投。
		// 事务保证「记 pending」和「记待发消息」同生共死，消息不会丢。
		payload, err := mq.MarshalOutboxPayload(jobMsg.JobID, jobMsg.TaskName, jobMsg.ConnectionID, jobMsg.Attempt, lease.Milliseconds())
		if err != nil {
			log.Printf("[Worker] 构造 outbox payload 失败: %v", err)
			msg.Nack(false, false)
			return
		}
		tx, err := h.db.Begin(context.Background())
		if err != nil {
			log.Printf("[Worker]开启事务失败: %v", err)
			msg.Nack(false, false)
			return
		}
		defer tx.Rollback(context.Background())

		if err := jobStore.UpdateStatusInTx(context.Background(), tx, jobMsg.JobID, model.JobStatusPending, job.TotalCount, job.Cursor, "connection 被占用，已延迟重投"); err != nil {
			log.Printf("[Worker] 记录 pending 失败: %v", err)
			msg.Nack(false, false)
			return
		}

		if err := h.outbox.Insert(context.Background(), tx, payload); err != nil {
			log.Printf("[Worker] 写 outbox 失败: %v", err)
			msg.Nack(false, false)
			return
		}
		if err := tx.Commit(context.Background()); err != nil {
			log.Printf("[Worker] 提交事务失败: %v", err)
			msg.Nack(false, false)
			return
		}
		log.Printf("[Worker] job_id=%d 新触发，已记录 pending 并延迟重投", jobMsg.JobID)
		msg.Ack(false)
		return
	}
	// 4. 抢到锁：taskCtx 和 hbCtx 分离
	//
	// taskCtx 传给 eng.Run，Renew 失败时 cancelTask 停任务；
	// hbCtx 只控制心跳 goroutine，正常收尾时 cancelHb 停心跳。
	// 两者分离，避免「正常结束 cancel 任务」被误判成 Renew 失败。
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
	runErr := h.runSync(taskCtx, jobMsg)

	// 6. 正常收尾：停心跳 → 等心跳退出 → 释放锁
	cancelHb()
	<-renewDone
	h.unlock(jobMsg.ConnectionID, credential)

	// 7. 判定最终结果
	if leaseLost.Load() {
		// 租约丢了，这次同步不可信（可能别的 Worker 已接管），进 DLQ
		log.Printf("[Worker] job_id=%d 租约丢失，进 DLQ", jobMsg.JobID)
		msg.Nack(false, false)
		return
	}
	if runErr != nil {
		// 熔断：数据源冷却中，按冷却时长写 outbox 延迟重投。
		// 事务保证「记 pending」和「记待发消息」同生共死。
		if errors.Is(runErr, circuitbreaker.ErrCircuitOpen) {
			latest, err := jobStore.GetByID(context.Background(), jobMsg.JobID)
			if err != nil {
				log.Printf("[Worker] 查询任务 %d 失败: %v", jobMsg.JobID, err)
				msg.Nack(false, false)
				return
			}
			delay := circuitbreaker.DefaultConfig().OpenTimeout //60s，与熔断冷却对齐
			payload, err := mq.MarshalOutboxPayload(jobMsg.JobID, jobMsg.TaskName, jobMsg.ConnectionID, jobMsg.Attempt+1, delay.Milliseconds())
			if err != nil {
				log.Printf("[Worker] 构造 outbox payload 失败: %v", err)
				msg.Nack(false, false)
				return
			}
			tx, err := h.db.Begin(context.Background())
			if err != nil {
				log.Printf("[Worker] 开启事务失败: %v", err)
				msg.Nack(false, false)
				return
			}
			defer tx.Rollback(context.Background())

			if err := jobStore.UpdateStatusInTx(context.Background(), tx, jobMsg.JobID, model.JobStatusPending, latest.TotalCount, latest.Cursor, "熔断中，冷却后重试"); err != nil {
				log.Printf("[Worker] 记录 pending 失败: %v", err)
				msg.Nack(false, false)
				return
			}
			if err := h.outbox.Insert(context.Background(), tx, payload); err != nil {
				log.Printf("[Worker] 写 outbox 失败: %v", err)
				msg.Nack(false, false)
				return
			}
			if err := tx.Commit(context.Background()); err != nil {
				log.Printf("[Worker] 提交事务失败: %v", err)
				msg.Nack(false, false)
				return
			}
			log.Printf("[Worker] job_id=%d 熔断中，%s 后重试", jobMsg.JobID, delay)
			msg.Ack(false)
			return
		}

		// 失败分流：
		//   永久错误（数据源配置错）→ 直接 DLQ，重试无意义
		if errors.Is(runErr, engine.ErrNonRetryable) {
			log.Printf("[Worker] job_id=%d 永久失败，进 DLQ: %v", jobMsg.JobID, runErr)
			msg.Nack(false, false)
			return
		}

		//   重试次数到上限 → DLQ 放弃
		if jobMsg.Attempt >= mq.MaxAttempts {
			log.Printf("[Worker] job_id=%d 重试 %d 次仍失败，进 DLQ: %v", jobMsg.JobID, jobMsg.Attempt, runErr)
			msg.Nack(false, false)
			return
		}

		// 可重试错误（熔断/网络/DB 抖动）→ 指数退避重投
		// 事务保证「记 pending」和「记待发消息」同生共死。
		delay := mq.BackoffDelay(jobMsg.Attempt)
		latest, err := jobStore.GetByID(context.Background(), jobMsg.JobID)
		if err != nil {
			log.Printf("[Worker] 查询任务 %d 失败: %v", jobMsg.JobID, err)
			msg.Nack(false, false)
			return
		}
		payload, err := mq.MarshalOutboxPayload(jobMsg.JobID, jobMsg.TaskName, jobMsg.ConnectionID, jobMsg.Attempt+1, delay.Milliseconds())
		if err != nil {
			log.Printf("[Worker] 构造 outbox payload 失败: %v", err)
			msg.Nack(false, false)
			return
		}
		tx, err := h.db.Begin(context.Background())
		if err != nil {
			log.Printf("[Worker] 开启事务失败: %v", err)
			msg.Nack(false, false)
			return
		}
		defer tx.Rollback(context.Background())

		if err := jobStore.UpdateStatusInTx(context.Background(), tx, jobMsg.JobID, model.JobStatusPending, latest.TotalCount, latest.Cursor, "执行失败，退避重试"); err != nil {
			log.Printf("[Worker] 记录 pending 失败: %v", err)
			msg.Nack(false, false)
			return
		}
		if err := h.outbox.Insert(context.Background(), tx, payload); err != nil {
			log.Printf("[Worker] 写 outbox 失败: %v", err)
			msg.Nack(false, false)
			return
		}
		if err := tx.Commit(context.Background()); err != nil {
			log.Printf("[Worker] 提交事务失败: %v", err)
			msg.Nack(false, false)
			return
		}
		log.Printf("[Worker] job_id=%d 第 %d 次失败，%s 后重试", jobMsg.JobID, jobMsg.Attempt, delay)
		msg.Ack(false)
		return
	}
	log.Printf("[Worker] job_id=%d 执行完成", jobMsg.JobID)
	msg.Ack(false)
}

// runSync 组装数据源和目标端，并执行同步引擎。
// ctx 会向下传给数据源、数据库、分页循环，Renew 失败 cancel 后能真正停下。
func (h *Handler) runSync(ctx context.Context, jobMsg mq.JobMessage) error {
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

	log.Printf("[Worker] 开始执行 job_id=%d task_name=%s", jobMsg.JobID, jobMsg.TaskName)
	return eng.Run(ctx, jobMsg.TaskName)
}

// unlock 用独立短超时 ctx 释放锁，不复用已经 cancel 掉的业务 ctx。
// 失败只记录日志，不重跑任务；锁会靠 TTL 自动过期兜底。
func (h *Handler) unlock(connectionID int, credential string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := h.connLock.Unlock(ctx, connectionID, credential); err != nil {
		log.Printf("[Worker] Unlock 失败（靠 TTL 释放）: %v", err)
	}
}
