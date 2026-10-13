package config

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	DatabaseURL string
	RabbitURL   string
	JWTSecret   string
	RedisAddr   string
	RedisPass   string
	Port        string // 默认 9090
	GitHubToken string
}

func Load(needJWT, needRedis bool) (*Config, error) {
	if err := godotenv.Load(); err != nil {
		slog.Warn("未加载 .env 文件，使用进程环境变量", "err", err)
	}

	cfg := &Config{
		DatabaseURL: os.Getenv("DATABASE_URL"),
		RabbitURL:   os.Getenv("RABBITMQ_URL"),
		JWTSecret:   os.Getenv("JWT_SECRET"),
		RedisAddr:   os.Getenv("REDIS_ADDR"),
		RedisPass:   os.Getenv("REDIS_PASS"),
		Port:        os.Getenv("PORT"),
		GitHubToken: os.Getenv("GITHUB_TOKEN"),
	}
	if cfg.Port == "" {
		cfg.Port = "9090"
	}

	// 按需校验：缺谁报谁，一次报全
	var missing []string
	if cfg.DatabaseURL == "" {
		missing = append(missing, "DATABASE_URL")
	}
	if cfg.RabbitURL == "" {
		missing = append(missing, "RABBITMQ_URL")
	}
	if needJWT && cfg.JWTSecret == "" {
		missing = append(missing, "JWT_SECRET")
	}
	if needRedis && cfg.RedisAddr == "" {
		missing = append(missing, "REDIS_ADDR")
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("缺少必填环境变量: %s", strings.Join(missing, ", "))
	}
	return cfg, nil
}
