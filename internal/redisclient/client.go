package redisclient

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
)

// Client 封装 go-redis 客户端。
//
// 只负责连接管理（创建、验证、关闭），不写具体业务逻辑。
// 后面的乐观锁、熔断器、限流器都从它拿底层客户端。
type Client struct {
	client *redis.Client
}

// NewClient 创建 Redis 客户端并验证连接。
//
// 参数：
//
//	addr     — Redis 地址，格式 "host:port"
//	password — Redis 密码，无密码传 ""
//
// 要点：
//   - redis.NewClient 只创建客户端对象，不会真正连
//   - Ping 才是真正建立连接，网络不通/密码错在这里暴露
//   - Ping 失败要关闭客户端释放资源，再返回 error
func NewClient(ctx context.Context, addr, password string) (*Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       0,
	})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("Ping redis error : %w", err)
	}

	log.Println("redis connected")
	return &Client{client: client}, nil

}

// Client 返回底层 *redis.Client，供上层模块使用。
//
// 不直接暴露 client 字段，用方法访问（和 repository.DB.Pool() 一个套路）。
func (c *Client) Client() *redis.Client {
	return c.client
}

// Close 关闭客户端，释放连接。
func (c *Client) Close() error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}
