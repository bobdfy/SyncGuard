# 部署

## 依赖服务（PostgreSQL / RabbitMQ / Redis）

推荐用 Docker Compose 一次性启动全部依赖：

```bash
docker compose -f deploy/docker-compose.yml up -d
```

服务端口：

| 服务 | 端口 | 说明 |
|------|------|------|
| PostgreSQL | 5432 | 数据库 |
| RabbitMQ | 5672 / 15672 | AMQP / Web 管理界面 |
| Redis | 6379 | 分布式锁 + 熔断器状态 |

## 仅启动 PostgreSQL（手动方式）

```bash
docker run -d -p 5432:5432 --name postgres-syncguard \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=syncguard \
  -e POSTGRES_DB=postgres \
  postgres
```

## 应用配置

启动依赖后，在项目根目录按 `.env` 配置数据库 / 消息队列 / Redis 连接地址，然后分别运行：

```bash
# 依赖服务：server 提供 HTTP API，worker 消费队列执行同步
go run ./cmd/server
go run ./cmd/worker
```
