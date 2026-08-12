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
	DB         *repository.DB             // 供 ListRecords 创建 SyncedStore 用
	MQ         *mq.Producer               // V2：发消息到 RabbitMQ，替代原来的 goroutine
	Reconciler *reconciliation.Reconciler // V3：对账引擎
}

// NewJobHandler 创建 JobHandler。
// V2 增加了 mqProducer 参数：RunJob 不再异步 goroutine，
// 而是通过 MQ 发消息给 Worker 执行。
func NewJobHandler(jobStore *repository.JobStore, db *repository.DB, mqProducer *mq.Producer, reconciler *reconciliation.Reconciler) *JobHandler {
	return &JobHandler{
		JobStore:   jobStore,
		DB:         db,
		MQ:         mqProducer,
		Reconciler: reconciler,
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
		TargetConnectionID int    `json:"target_connection_id"`
		TaskName           string `json:"task_name"`
		SyncContent        string `json:"sync_content"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "JSON格式错误"})
		return
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
	// 1. 从 URL 取 id
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "路径不合法"})
		return
	}

	// 2. 调 Store 查询
	job, err := h.JobStore.GetByID(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询失败"})
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

// RunJob POST /api/jobs/:id/run → 启动同步任务（V2：发消息到 RabbitMQ）
//
// V2 改动：
//   - 不再创建 engine + goroutine
//   - 改为调 MQ.Publish() 把任务信息发到 RabbitMQ 队列
//   - Worker 进程收到消息后跑引擎
//   - HTTP 请求立即返回，不阻塞
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

	// ========== 4. V2：发消息到 RabbitMQ（替代原来的 goroutine） ==========
	// taskName 用 jobID 的字符串表示，传给 Worker 后填入 engine.Run()
	taskName := strconv.Itoa(id)

	// TODO: 填空 — 调 h.MQ.Publish(c.Request.Context(), id, taskName)
	// 如果 err != nil，返回 "发送消息失败"
	// 成功则返回 "任务已发送到队列"
	err = h.MQ.Publish(c.Request.Context(), id, taskName, job.ConnectionID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "发送消息失败"})
		return
	}
	// ========== 5. 打日志 ==========
	log.Printf("任务 %d 已投递到 MQ", id)
	c.JSON(http.StatusOK, gin.H{"data": "任务已发送"})
}

// ListRecords GET /api/records → 查看同步结果
func (h *JobHandler) ListRecords(c *gin.Context) {
	// 1. 取 limit + offset 参数
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
	// 2. 创建 SyncedStore
	syncedStore := repository.NewSyncedStore(h.DB)
	// 3. 调 store.ListRecords(ctx, limit, offset)
	list, err := syncedStore.ListRecords(c.Request.Context(), limit, offset)
	// 4. 返回 JSON
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
	// TODO: 填空1 — strconv.Atoi(c.Param("id"))
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
	// TODO: 填空2 — 调 h.Reconciler.Reconcile(...)
	// 提示：用 c.Request.Context() 作为 ctx，第二个参数是 job.ConnectionID
	diffs, err := h.Reconciler.Reconcile(c.Request.Context(), job.ConnectionID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "对账出现错误"})
	}

	// ========== 5. 返回差异 ==========
	c.JSON(http.StatusOK, gin.H{
		"job_id":           id,
		"connection_id":    job.ConnectionID,
		"difference_count": len(diffs),
		"data":             diffs,
	})
}
