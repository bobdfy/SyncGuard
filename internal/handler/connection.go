package handler

import (
	"net/http"
	"strconv"

	"github.com/bobdfy/syncguard/internal/repository"
	"github.com/gin-gonic/gin"
)

// ConnectionHandler 数据源 CRUD 的 HTTP handler
type ConnectionHandler struct {
	ConnStore *repository.ConnectionStore
}

func NewConnectionHandler(connStore *repository.ConnectionStore) *ConnectionHandler {
	return &ConnectionHandler{ConnStore: connStore}
}

// ListConnections GET /api/connections → 返回当前用户的所有数据源
func (h *ConnectionHandler) ListConnections(c *gin.Context) {
	// 1. 从 context 取 userID（中间件注入的）
	userID, ok := c.Get("userID")
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "未登录"})
		return
	}

	userIDint := userID.(int)

	// 2. 调 Store 查列表
	conns, err := h.ConnStore.ListByUser(c.Request.Context(), userIDint)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询失败"})
		return
	}

	// 3. 返回 JSON 数组（空列表也正常返回）
	c.JSON(http.StatusOK, gin.H{"data": conns})
}

// CreateConnection POST /api/connections → 创建新数据源
func (h *ConnectionHandler) CreateConnection(c *gin.Context) {
	// 1. 取 userID
	userID, ok := c.Get("userID")
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "未登录"})
		return
	}

	userIDint := userID.(int)

	// 2. 绑定 JSON 请求体
	var req struct {
		Name       string `json:"name"`
		SourceType string `json:"source_type"`
		SourceURL  string `json:"source_url"`
		CDCEnabled bool   `json:"cdc_enabled"`
		TargetURL  string `json:"target_url"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "JSON格式错误"})
		return
	}

	// 3. CDC 字段校验（前端虽校验，API 可被直接调用，后端必须兜底）
	if errMsg := validateCDC(req.SourceType, req.CDCEnabled, req.TargetURL); errMsg != "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
		return
	}

	// 4. 调 Store 写入
	err := h.ConnStore.Create(c.Request.Context(), userIDint, req.Name, req.SourceType, req.SourceURL, req.CDCEnabled, req.TargetURL)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "创建新数据源失败"})
		return
	}

	// 5. 返回成功
	c.JSON(http.StatusCreated, gin.H{"data": req})
}

// UpdateConnection PUT /api/connections/:id → 修改数据源
func (h *ConnectionHandler) UpdateConnection(c *gin.Context) {
	// 1. 取 userID
	userID, ok := c.Get("userID")
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "用户未登录"})
		return
	}

	userIDint := userID.(int)

	// 2. 从 URL 路径取 id，转成 int
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "路径不合法"})
		return
	}

	// 3. 绑定 JSON 请求体
	var req struct {
		Name       string `json:"name"`
		SourceType string `json:"source_type"`
		SourceURL  string `json:"source_url"`
		CDCEnabled bool   `json:"cdc_enabled"`
		TargetURL  string `json:"target_url"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "JSON格式错误"})
		return
	}

	// 4. CDC 字段校验（同 Create）
	if errMsg := validateCDC(req.SourceType, req.CDCEnabled, req.TargetURL); errMsg != "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
		return
	}

	// 5. 调 Store 更新
	err = h.ConnStore.Update(c.Request.Context(), id, userIDint, req.Name, req.SourceType, req.SourceURL, req.CDCEnabled, req.TargetURL)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "更新失败"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": req})
}

// validateCDC 校验 CDC 开关的合法性，返回空串表示通过。
// 仅 PostgreSQL 源可启用 CDC，且必须填写镜像目标库连接串。
// 前端虽已校验，但 API 可被直接调用，后端必须兜底——否则空 target_url 的连接
// 会让 cdcwriter 每条事件重试到死信；github 源开 CDC 会让 cdcreader 拿 owner/repo 当 DSN 刷错误。
func validateCDC(sourceType string, cdcEnabled bool, targetURL string) string {
	if !cdcEnabled {
		return ""
	}
	if sourceType != "postgres" {
		return "仅 PostgreSQL 数据源支持启用 CDC"
	}
	if targetURL == "" {
		return "启用 CDC 需要填写镜像目标库连接串"
	}
	return ""
}

// DeleteConnection DELETE /api/connections/:id → 删除数据源
func (h *ConnectionHandler) DeleteConnection(c *gin.Context) {
	// 1. 取 userID
	userID, ok := c.Get("userID")
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "获取id失败"})
		return
	}

	userIDint := userID.(int)

	// 2. 从 URL 路径取 id
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "获取id失败"})
		return
	}

	// 3. 调 Store 删除
	err = h.ConnStore.Delete(c.Request.Context(), id, userIDint)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "删除失败"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": "success"})
}
