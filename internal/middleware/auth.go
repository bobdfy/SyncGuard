package middleware

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

// JWTSecret 签发和验证 JWT 时用的密钥
// 生产环境应该从环境变量读取
const JWTSecret = "syncguard-secret-key"

// Claims 自定义 JWT payload，除了标准字段外带上 UserID 和 Username
type Claims struct {
	UserID   int    `json:"user_id"`
	Username string `json:"username"`
	jwt.RegisteredClaims
}

// AuthRequired 登录验证中间件
// 从 Authorization header 取出 Bearer token → 解析 → 把 userID、username 注入 context → 放行
// 没 token 或 token 无效 → 返回 401 JSON
func AuthRequired() gin.HandlerFunc {
	return func(c *gin.Context) {

		// 1. 从 Authorization header 取 token，格式：Bearer xxxxx
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.JSON(http.StatusUnauthorized, gin.H{
				"error": "未登录",
			})
			c.Abort()
			return
		}

		// 2. 去掉 "Bearer " 前缀
		tokenString := strings.TrimPrefix(authHeader, "Bearer ")
		if tokenString == authHeader {
			// 没有 Bearer 前缀 → 格式错误
			c.JSON(http.StatusUnauthorized, gin.H{
				"error": "格式错误",
			})
			c.Abort()
			return
		}

		// 3. 解析 JWT，Claims 用上面定义的结构体
		claims := &Claims{}
		token, err := jwt.ParseWithClaims(tokenString, claims,
			func(t *jwt.Token) (any, error) {
				return []byte(JWTSecret), nil
			},
		)

		// 4. 解析失败或 token 无效 → 401
		if err != nil || !token.Valid {
			c.JSON(http.StatusUnauthorized, gin.H{
				"error": "token无效",
			})
			c.Abort()
			return
		}

		// 5. 把 userID 和 username 写入 context，后面的 handler 通过 c.Get 取
		c.Set("userID", claims.UserID)
		c.Set("username", claims.Username)

		// 6. 放行
		c.Next()
	}
}

// GenerateToken 用 userID 和 username 签发一个新的 JWT token 字符串
func GenerateToken(userID int, username string) (string, error) {
	claims := &Claims{
		UserID:   userID,
		Username: username,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	tokenString, err := token.SignedString([]byte(JWTSecret))
	if err != nil {
		return "", err
	}
	return tokenString, nil
}
