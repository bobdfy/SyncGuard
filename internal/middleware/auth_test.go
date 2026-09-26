package middleware

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

// TestSetJWTSecret 验证密钥校验：空密钥必须拒绝，防止用空密钥签发 token。
func TestSetJWTSecret(t *testing.T) {
	if err := SetJWTSecret(""); err == nil {
		t.Fatal("期望空密钥被拒绝，实际成功")
	}
	if err := SetJWTSecret("test-secret"); err != nil {
		t.Fatalf("期望正常密钥成功，实际: %v", err)
	}
}

// TestGenerateToken 验证签发的 token 能解析回正确的 Claims。
func TestGenerateToken(t *testing.T) {
	if err := SetJWTSecret("test-secret"); err != nil {
		t.Fatalf("设置密钥失败: %v", err)
	}

	tokenString, err := GenerateToken(42, "alice")
	if err != nil {
		t.Fatalf("GenerateToken 报错: %v", err)
	}

	claims := &Claims{}
	token, err := jwt.ParseWithClaims(tokenString, claims, func(t *jwt.Token) (any, error) {
		return []byte("test-secret"), nil
	})
	if err != nil || !token.Valid {
		t.Fatalf("token 解析失败: %v", err)
	}
	if claims.UserID != 42 || claims.Username != "alice" {
		t.Errorf("Claims 不一致: 期望 userID=42 username=alice，实际 %+v", claims)
	}
}

// TestAuthRequired 验证鉴权中间件的四条路径。
func TestAuthRequired(t *testing.T) {
	gin.SetMode(gin.TestMode)
	if err := SetJWTSecret("test-secret"); err != nil {
		t.Fatalf("设置密钥失败: %v", err)
	}

	// 受保护路由：放行后把注入的 userID/username 原样返回
	router := gin.New()
	router.GET("/protected", AuthRequired(), func(c *gin.Context) {
		userID, _ := c.Get("userID")
		username, _ := c.Get("username")
		c.JSON(http.StatusOK, gin.H{"userID": userID, "username": username})
	})

	doRequest := func(authHeader string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/protected", nil)
		if authHeader != "" {
			req.Header.Set("Authorization", authHeader)
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		return w
	}

	t.Run("无 Authorization header → 401", func(t *testing.T) {
		w := doRequest("")
		if w.Code != http.StatusUnauthorized {
			t.Errorf("期望 401，实际 %d", w.Code)
		}
	})

	t.Run("缺 Bearer 前缀 → 401", func(t *testing.T) {
		w := doRequest("just-a-token")
		if w.Code != http.StatusUnauthorized {
			t.Errorf("期望 401，实际 %d", w.Code)
		}
		if !strings.Contains(w.Body.String(), "格式错误") {
			t.Errorf("期望提示格式错误，实际返回: %s", w.Body.String())
		}
	})

	t.Run("无效 token → 401", func(t *testing.T) {
		w := doRequest("Bearer not.a.valid.token")
		if w.Code != http.StatusUnauthorized {
			t.Errorf("期望 401，实际 %d", w.Code)
		}
	})

	t.Run("有效 token → 放行且注入 userID/username", func(t *testing.T) {
		token, err := GenerateToken(7, "bob")
		if err != nil {
			t.Fatalf("GenerateToken 报错: %v", err)
		}
		w := doRequest("Bearer " + token)
		if w.Code != http.StatusOK {
			t.Fatalf("期望 200，实际 %d，返回: %s", w.Code, w.Body.String())
		}
		body := w.Body.String()
		if !strings.Contains(body, `"userID":7`) || !strings.Contains(body, `"username":"bob"`) {
			t.Errorf("期望注入 userID=7 username=bob，实际返回: %s", body)
		}
	})
}
