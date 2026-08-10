package handler

import (
	"net/http"

	"github.com/bobdfy/syncguard/internal/middleware"
	"github.com/bobdfy/syncguard/internal/repository"
	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"
)

// AuthHandler 登录/注册/退出相关的 HTTP handler
// 持有 UserStore 用来查用户
type AuthHandler struct {
	UserStore *repository.UserStore
}

func NewAuthHandler(userStore *repository.UserStore) *AuthHandler {
	return &AuthHandler{UserStore: userStore}
}

// Login POST /api/login → 验证用户名密码 → 返回 JWT token
func (h *AuthHandler) Login(c *gin.Context) {
	// 1. 取表单参数
	username := c.PostForm("username")
	password := c.PostForm("password")

	// 2. 空值判断
	if username == "" || password == "" {
		// 改：返回 JSON 错误，HTTP 400
		c.JSON(400, gin.H{
			"error": "用户名或密码不能为空",
		})
		return
	}

	// 3. 从数据库查用户（按 username）
	user, err := h.UserStore.GetByUsername(c.Request.Context(), username)
	if err != nil {
		// 改：返回 JSON 错误，HTTP 401（不透露用户名还是密码错了）
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "用户名或者密码错误",
		})
		return
	}

	// 4. 用 bcrypt 比对：用户输入的密码 vs 数据库存的哈希
	err = bcrypt.CompareHashAndPassword(
		[]byte(user.PasswordHash),
		[]byte(password),
	)
	if err != nil {
		// 改：返回 JSON 错误，HTTP 401
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "用户名或密码错误",
		})
		return
	}

	// 5. 签发 JWT token
	token, err := middleware.GenerateToken(int(user.ID), user.UserName)
	if err != nil {
		// 改：返回 JSON 错误，HTTP 500
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "签发token失败",
		})
		return
	}

	// 6. 改：返回 JSON，把 token 和 username 给前端
	//    前端收到后存 localStorage，后续请求带在 Authorization header 里
	c.JSON(http.StatusCreated, gin.H{
		"token":    token,
		"username": user.UserName,
	})
}

// Register POST /api/register → 创建用户 → 返回 JWT token（注册即登录）
func (h *AuthHandler) Register(c *gin.Context) {
	// 1. 取表单参数
	username := c.PostForm("username")
	password := c.PostForm("password")

	// 2. 空值判断
	if username == "" || password == "" {
		// 改：返回 JSON 错误，HTTP 400
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "用户名或密码不能为空",
		})
		return
	}

	// 3. 用 bcrypt 生成密码哈希（Cost=10 是默认强度）
	hashBytes, err := bcrypt.GenerateFromPassword([]byte(password), 10)
	if err != nil {
		// 改：返回 JSON 错误，HTTP 500
		c.JSON(500, gin.H{
			"error": "密码无效",
		})
		return
	}

	// 4. 写入数据库
	err = h.UserStore.Create(c.Request.Context(), username, string(hashBytes))
	if err != nil {
		// 改：返回 JSON 错误，HTTP 409（Conflict，用户名重复）
		c.JSON(http.StatusConflict, gin.H{
			"error": "用户名已存在",
		})
		return
	}

	// 5. 查回用户的 ID（自增生成的，注册时不知道）
	user, err := h.UserStore.GetByUsername(c.Request.Context(), username)
	if err != nil {
		// 改：返回 JSON 错误，HTTP 500
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询失败"})
		return
	}

	// 6. 签发 JWT
	token, err := middleware.GenerateToken(int(user.ID), user.UserName)
	if err != nil {
		// 改：返回 JSON 错误，HTTP 500
		c.JSON(http.StatusInternalServerError, gin.H{"error": "签发JWT失效"})
		return
	}

	// 7. 改：返回 JSON（注册即登录，不需要再跳登录页）
	c.JSON(http.StatusOK, gin.H{
		"token":    token,
		"username": user.UserName,
	})
}

// Logout GET /api/logout → 退出登录
// JWT 是无状态的，服务端不需要删任何东西，前端自己删 localStorage 即可
func (h *AuthHandler) Logout(c *gin.Context) {
	// 改：返回 JSON 确认（以前是 c.Redirect 跳登录页）
	c.JSON(http.StatusOK, gin.H{"data": ""})
}
