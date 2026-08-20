# SyncGuard

一个**分布式数据同步系统**（Go 实现）：把数据从「源端」可靠地搬到「目标端」，支持多数据源、崩溃恢复、多 Worker 协调与数据对账。

## 功能特性

- **断点续传 + 幂等写入**：每页数据写完后落库游标，崩溃重启后从断点继续；`ON CONFLICT DO UPDATE` 保证重复写入结果一致，最多丢失/重写一页。
- **Server / Worker 解耦**：Server 负责 HTTP API 并将任务发布到 RabbitMQ 后立即返回；Worker 后台消费并执行同步，长任务不阻塞请求。
- **分布式锁 + 心跳续约**：基于 Redis（`SET NX EX` + Lua 原子脚本）实现连接级锁，Worker 执行期间每 10 秒续约，防止多 Worker 重复同步同一数据源；credential 校验防止误删他人锁。
- **熔断器 + 指数退避重试**：三态熔断器（closed / open / half_open）在数据源持续故障时快速失败；可重试失败按指数退避（5s 起、2min 封顶、带随机抖动）延迟重投；永久错误直接进入死信队列（DLQ）。
- **数据对账**：对比源端与目标端，检测四类差异：`missing_in_target` / `extra_in_target` / `version_mismatch` / `content_mismatch`。
- **多用户隔离 + JWT 鉴权**：数据源、任务、同步数据均按用户隔离；注册/登录采用 bcrypt + JWT。
- **单元测试**：覆盖引擎、分布式锁、熔断器、退避计算等核心模块。

## 架构

```
浏览器（静态页）
   │ HTTP + JWT
   ▼
┌───────────┐   发布消息    ┌──────────┐   消费    ┌───────────┐
│  Server   │ ────────────► │ RabbitMQ │ ────────► │  Worker   │
│ (Gin API) │               │  (队列)   │           │ (执行同步) │
└─────┬─────┘               └──────────┘           └─────┬─────┘
      │ 读写                       │ 锁 / 熔断状态         │ 读写
      ▼                            ▼                      ▼
┌───────────┐               ┌───────────┐
│ PostgreSQL │               │   Redis   │
└───────────┘               └───────────┘
```

## 技术栈

- **语言**：Go 1.26
- **HTTP**：Gin
- **数据库**：PostgreSQL（pgx）
- **消息队列**：RabbitMQ（amqp091-go）
- **缓存/协调**：Redis（go-redis，分布式锁 + 熔断状态）
- **鉴权**：JWT + bcrypt

## 快速开始

详细部署说明（依赖端口、手动启动方式）见 [deploy/README.md](deploy/README.md)。核心三步：

1. 启动依赖（PostgreSQL / RabbitMQ / Redis）：

```bash
docker compose -f deploy/docker-compose.yml up -d
```

2. 执行数据库迁移（`migrations/` 下 1 个 SQL 文件 `001_init.sql`，导入 PostgreSQL；脚本会先 DROP 旧表再重建）。

3. 配置 `.env`（需含 `JWT_SECRET`，服务启动时校验，空值拒绝启动），然后启动两个进程：

```bash
go run ./cmd/server   # HTTP 服务，默认 :9090
go run ./cmd/worker   # 消费队列执行同步
```

打开 `http://localhost:9090`：注册账号 → 创建数据源（Mock / GitHub）→ 创建任务并启动 → 查看同步结果 / 对账。

## 目录结构

```
cmd/
  server/        HTTP 服务入口（装配 + 路由 + 优雅退出）
  worker/        消费进程入口（装配 + 消息循环）
internal/
  model/         领域模型（Record / SyncJob / Connection / User / Diff）
  engine/        同步引擎（Source/Destination 接口 + 断点续传分页循环）
  source/        数据源工厂 + mock / github 实现
  repository/    数据访问层（连接池 + 各表 Store + Destination 实现）
  mq/            RabbitMQ 拓扑、Producer/Consumer、退避计算
  lock/          Redis 分布式锁 + 心跳续约
  circuitbreaker/ 三态熔断器 + 装饰器
  reconciliation/ 对账引擎
  redisclient/   Redis 客户端封装
  handler/       HTTP 处理器（鉴权 / 数据源 / 任务 / 记录 / 对账）
  middleware/    JWT 鉴权中间件
  worker/        消息处理编排（抢锁 → 续约 → 执行 → 分流）
migrations/      SQL 迁移文件
web/             前端静态页面（登录 / 数据源 / 任务 / 结果）
deploy/          docker-compose 与部署说明
```

## 测试

```bash
go test ./...
```

覆盖：

| 包 | 验证内容 |
|----|---------|
| engine | 完整同步、断点续传、幂等、失败标记 |
| lock | 抢锁、互斥、释放后可再抢 |
| circuitbreaker | 连续失败熔断、冷却后放探针、探针成功复原 |
| mq | 退避延迟下限/封顶/递增 |
| source/mock | 分页正确性 |

## Roadmap

- [ ] PostgreSQL 数据源（表级同步）
- [ ] 目标端抽象（写入外部 PG 等，当前默认写入内部存储）
- [ ] Outbox 模式（写库与发消息同事务，保证消息不丢）
- [ ] 部署上云（Dockerfile + 容器化编排）

## License

个人学习项目。
