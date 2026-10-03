package handler

import (
	"log"
	"net/http"
	"strconv"

	"github.com/bobdfy/syncguard/internal/mq"
	"github.com/bobdfy/syncguard/internal/reconciliation"
	"github.com/bobdfy/syncguard/internal/repository"
	"github.com/gin-gonic/gin"
)

// JobHandler 同步任务 CRUD + 启动同步的 HTTP handler
type JobHandler struct {
	JobStore   *repository.JobStore
	ConnStore  *repository.ConnectionStore
	DB         *repository.DB             // 供 ListRecords 创建 SyncedStore 用
	MQ         *mq.Producer               // V2：发消息到 RabbitMQ，替代原来的 goroutine
	Reconciler *reconciliation.Reconciler // V3：对账引擎
	Outbox     *repository.OutboxStore
}

// NewJobHandler 创建 JobHandler。
// V2 增加了 mqProducer 参数：RunJob 不再异步 goroutine，
// 而是通过 MQ 发消息给 Worker 执行。
func NewJobHandler(jobStore *repository.JobStore, connStore *repository.ConnectionStore, db *repository.DB, mqProducer *mq.Producer, reconciler *reconciliation.Reconciler, outbox *repository.OutboxStore) *JobHandler {
	return &JobHandler{
		JobStore:   jobStore,
		ConnStore:  connStore,
		DB:         db,
		MQ:         mqProducer,
		Reconciler: reconciler,
		Outbox:     outbox,
	}
}

// ListJobs GET /api/jobs → 返回当前用户的所有同步任务
func (h *JobHandler) ListJobs(c *gin.Context) {
	userID, ok := c.Get("userID")
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "未登录"})
		return
	}

	userIDint := userID.(int)

	jobs, err := h.JobStore.ListByUser(c.Request.Context(), userIDint)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询失败"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": jobs})
}

// CreateJob POST /api/jobs → 创建新同步任务
func (h *JobHandler) CreateJob(c *gin.Context) {
	// 1. 取 userID
	userID, ok := c.Get("userID")
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "未登录"})
		return
	}

	userIDint := userID.(int)

	// 2. 绑定 JSON 请求体
	var req struct {
		ConnectionID       int    `json:"connection_id"`
		TargetConnectionID *int   `json:"target_connection_id"` // 可空：nil = 默认内部存储
		TaskName           string `json:"task_name"`
		SyncContent        string `json:"sync_content"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "JSON格式错误"})
		return
	}

	conn, err := h.ConnStore.GetByID(c.Request.Context(), req.ConnectionID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "数据源不存在"})
		return
	}
	if conn.UserID != userIDint {
		c.JSON(http.StatusForbidden, gin.H{"error": "无权使用该数据源"})
		return
	}

	// 校验目标数据源归属（如果填了）
	if req.TargetConnectionID != nil {
		targetConn, err := h.ConnStore.GetByID(c.Request.Context(), *req.TargetConnectionID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "目标数据源不存在"})
			return
		}
		if targetConn.UserID != userIDint {
			c.JSON(http.StatusForbidden, gin.H{"error": "无权使用该目标数据源"})
			return
		}
	}

	// 3. 调 Store 写入
	jobID, err := h.JobStore.Create(c.Request.Context(), userIDint, req.ConnectionID, req.TargetConnectionID, req.TaskName, req.SyncContent)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "创建失败"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"data": gin.H{"id": jobID}})
}

// GetJob GET /api/jobs/:id → 查看单条任务详情
func (h *JobHandler) GetJob(c *gin.Context) {
	// 1. 取 userID
	userID, ok := c.Get("userID")
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "未登录"})
		return
	}
	userIDint := userID.(int)

	// 2. 从 URL 取 id
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "路径不合法"})
		return
	}

	// 3. 调 Store 查询
	job, err := h.JobStore.GetByID(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "任务不存在"})
		return
	}
	if job.UserID != userIDint {
		c.JSON(http.StatusForbidden, gin.H{"error": "无权操作"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": job})
}

// DeleteJob DELETE /api/jobs/:id → 删除任务
func (h *JobHandler) DeleteJob(c *gin.Context) {
	// 1. 取 userID
	userID, ok := c.Get("userID")
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "未登录"})
		return
	}

	userIDint := userID.(int)

	// 2. 取 id
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "路径不合法"})
		return
	}

	// 3. 调 Store 删除
	err = h.JobStore.Delete(c.Request.Context(), id, userIDint)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "删除失败"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": "删除成功"})
}

// RunJob POST /api/jobs/:id/run → 启动同步任务
//
//	调 MQ.Publish() 把任务信息发到 RabbitMQ 队列
//	- Worker 进程收到消息后跑引擎
//	- HTTP 请求立即返回，不阻塞
func (h *JobHandler) RunJob(c *gin.Context) {
	// ========== 1. 取 userID（JWT 中间件注入） ==========
	userID, ok := c.Get("userID")
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "未登录"})
		return
	}
	userIDint := userID.(int)

	// ========== 2. 从 URL 取 jobID ==========
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "路径不合法"})
		return
	}

	// ========== 3. 查 DB：任务存在 + 属于当前用户 ==========
	job, err := h.JobStore.GetByID(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "任务不存在"})
		return
	}
	if job.UserID != userIDint {
		c.JSON(http.StatusForbidden, gin.H{"error": "无权操作"})
		return
	}

	//4. 写 outbox：搬运工负责发消息，消息不丢
	// taskName 用 jobID 的字符串表示，传给 Worker 后填入 engine.Run()
	taskName := strconv.Itoa(id)

	// DelayMs=0 表示搬运工扫到后立即发主队列
	payload, err := mq.MarshalOutboxPayload(id, taskName, job.ConnectionID, 0, 0)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "构造消息失败"})
		return
	}

	tx, err := h.DB.Begin(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "开启事务失败"})
		return
	}
	defer tx.Rollback(c.Request.Context())

	if err := h.Outbox.Insert(c.Request.Context(), tx, payload); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "记录消息失败"})
		return
	}

	if err := tx.Commit(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "提交事务失败"})
		return
	}

	// ========== 5. 打日志 ==========
	log.Printf("任务 %d 已投递到 MQ", id)
	c.JSON(http.StatusOK, gin.H{"data": "任务已发送"})
}

// ListRecords GET /api/records → 查看同步结果
func (h *JobHandler) ListRecords(c *gin.Context) {

	userID, ok := c.Get("userID")
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "未登录"})
		return
	}
	userIDint := userID.(int)
	//  取 limit + offset 参数
	limitStr := c.DefaultQuery("limit", "50")
	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "参数格式错误"})
		return
	}
	offsetStr := c.DefaultQuery("offset", "0")
	offset, err := strconv.Atoi(offsetStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "参数格式错误"})
		return
	}
	// 创建 SyncedStore
	syncedStore := repository.NewSyncedStore(h.DB)
	// 调 store.ListRecords(ctx, limit, offset)
	list, err := syncedStore.ListRecordsByUser(c.Request.Context(), userIDint, limit, offset)
	// 返回 JSON
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询失败"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": list})
}

// Reconcile POST /api/jobs/:id/reconcile → 对账
//
// 流程：
//  1. 从 URL 取 jobID
//  2. 查 DB 拿到 job，验权，取出 ConnectionID
//  3. 调 Reconciler.Reconcile(ctx, connectionID) 执行对账
//  4. 返回差异列表 JSON
func (h *JobHandler) Reconcile(c *gin.Context) {
	// ========== 1. 取 userID（JWT 中间件注入） ==========
	userID, ok := c.Get("userID")
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "未登录"})
		return
	}
	userIDint := userID.(int)

	// ========== 2. 从 URL 取 jobID ==========
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "参数格式错误"})
		return
	}

	// ========== 3. 查 DB：任务存在 + 属于当前用户 ==========
	job, err := h.JobStore.GetByID(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "任务不存在"})
		return
	}
	if job.UserID != userIDint {
		c.JSON(http.StatusForbidden, gin.H{"error": "无权操作"})
		return
	}

	// ========== 4. 执行对账 ==========
	diffs, err := h.Reconciler.Reconcile(c.Request.Context(), job.ConnectionID, userIDint, job.SyncContent, job.TargetConnectionID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "对账出现错误"})
		return
	}

	// ========== 5. 返回差异 ==========
	c.JSON(http.StatusOK, gin.H{
		"job_id":           id,
		"connection_id":    job.ConnectionID,
		"difference_count": len(diffs),
		"data":             diffs,
	})
}
