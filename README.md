# SyncGuard

一个**分布式数据同步系统**（Go 实现）：把数据从「源端」可靠地搬到「目标端」，支持多数据源、可插拔目标端、崩溃恢复、多 Worker 协调、数据对账，以及基于 PostgreSQL 逻辑复制的**真 CDC 增量同步**（多租户路由）。

![Go](https://img.shields.io/badge/Go-1.26-00ADD8?style=flat&logo=go&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-blue)

---

## 目录

- [功能特性](#功能特性)
- [架构](#架构)
- [数据源与目标端](#数据源与目标端)
- [CDC 增量同步](#cdc-增量同步)
- [技术栈](#技术栈)
- [快速开始](#快速开始)
- [环境变量](#环境变量)
- [命令行工具](#命令行工具)
- [API 接口](#api-接口)
- [目录结构](#目录结构)
- [测试](#测试)
- [常见问题](#常见问题)
- [Roadmap](#roadmap)
- [License](#license)

---

## 功能特性

- **断点续传 + 幂等写入**：每页数据写完后落库游标（含最后一页），崩溃重启后从断点继续；`ON CONFLICT DO UPDATE` 保证重复写入结果一致，最多丢失/重写一页。
- **可插拔数据源 / 目标端**：Source / Destination 接口化，由工厂按类型创建。数据源支持 Mock / GitHub / PostgreSQL（表级增量同步）；目标端支持内部存储（`synced_records`）或外部 PostgreSQL 表，任意组合。
- **两条轨道（信封 / 原表镜像）**：源是数据库（PostgreSQL），就镜像成原表结构——内省源表（列名、类型、主键，支持复合主键）→ 目标库自动建同构表 → 列对列原生类型写入（`numeric` / `jsonb` / `timestamptz` 等保真）；源是 API / 文档类（Mock / GitHub），才用通用信封 `id / version / updated_at / data(JSONB)`。
- **真 CDC 增量同步**：基于 pglogrepl 逻辑复制解码 WAL（非轮询，能捕获 DELETE），事件经「WAL → cdc_events 事件表（事务内含 LSN 断点）→ MQ → 目标库」两段式落地；多租户场景下每个连接独立 replication slot + reader，事件按 `connection_id` 路由到各自目标库。详见 [CDC 增量同步](#cdc-增量同步)。
- **事务性 Outbox**：任务入库与"投递消息"在同一数据库事务中完成，Worker 内的 Dispatcher 轮询 outbox 表再投递到 RabbitMQ（支持延迟消息），从根上避免"任务已创建但消息丢失"；失败退避重投、熔断重投同样走延迟消息。
- **Server / Worker 解耦**：Server 负责 HTTP API 并把任务写入 outbox 后立即返回；Worker 后台消费并执行同步，长任务不阻塞请求。
- **分布式锁 + 心跳续约**：基于 Redis（`SET NX EX` + Lua 原子脚本）实现连接级锁，Worker 执行期间每 10 秒续约，防止多 Worker 重复同步同一数据源；credential 校验防止误删他人锁。
- **熔断器 + 指数退避重试**：基于 Redis 状态共享的三态熔断器（closed / open / half_open）在数据源持续故障时快速失败；可重试失败按指数退避（5 s 起、2 min 封顶、带随机抖动）经**分档延迟队列**重投（队列级 TTL，避免队头阻塞）；永久错误与重试超限进入死信队列（DLQ），附一次性补投工具（见[命令行工具](#命令行工具)）。
- **数据对账**：对比源端与目标端，检测四类差异：`missing_in_target` / `extra_in_target` / `version_mismatch` / `content_mismatch`。
- **多用户隔离 + JWT 鉴权**：数据源、任务、同步数据均按用户隔离；注册/登录采用 bcrypt + JWT（HS256 固定算法）。

## 架构

```
浏览器（静态页）
   │ HTTP + JWT
   ▼
┌───────────────┐
│  Server       │  注册/登录、数据源/任务管理、结果查询
│  (Gin API)    │
└───────┬───────┘
        │ 写库与"发消息"同一事务（Transactional Outbox）
        ▼
┌──────────────────────────┐
│  PostgreSQL（中央库）     │
│  业务表 + outbox + cdc_events │
└───────┬──────────────────┘
        │ Worker 内 Dispatcher 每 2s 轮询 outbox，按 DelayMs 投递
        ▼
┌───────────────┐   消费    ┌───────────────┐
│  RabbitMQ     │ ────────► │  Worker       │
│ 主队列/分档延迟/死信│       │ 抢锁→续约→同步 │
└───────────────┘           └───────┬───────┘
                                   │ 读写
              ┌────────────────────┼───────────────────┐
              ▼                    ▼                   ▼
      ┌───────────────┐    ┌───────────────┐   ┌─────────────────┐
      │ 内部存储       │    │ 外部目标端     │   │  Redis          │
      │ synced_records│    │ (PostgreSQL)  │   │ 锁/熔断状态/续约 │
      └───────────────┘    └───────────────┘   └─────────────────┘

对账（reconciliation）：任务结束后按用户/连接对比源端与目标端，写入 diff 表供查询。
```

CDC 链路（与上面的轮询同步相互独立，见下一节）：

```
源库 A ──WAL──► cdcreader  ─┐                  ┌─► 目标库 A
（slot_a，每连接一个 reader）│  写 cdc_events   │
源库 B ──WAL──► cdcreader  ─┼─►（中央库，含     ├─► 目标库 B
（slot_b）                  │  connection_id） │
                            ▼                 │
                    cdc dispatcher ─► MQ ─► cdcwriter（按 connection_id
                                             解析各自目标库，幂等应用）
```

## 数据源与目标端

### 数据源（`source_type` + `source_url`）

| 类型 | `source_url` 格式 | 同步内容（`sync_content`） | 说明 |
|------|-------------------|---------------------------|------|
| `mock` | 不需要 | 不需要 | 生成 1000 条测试数据，用于本地联调 |
| `github` | `owner/repo`，如 `golang/go` | 不需要 | 拉取仓库 Issues（自动过滤 PR），走「信封轨道」 |
| `postgres` | 数据库连接串 | `表名[:主键列]`，如 `users:id` | 表级增量同步，走「原表镜像轨道」（以源表真实主键为准，支持复合主键）；可额外启用 CDC |

> GitHub 源建议配置 `GITHUB_TOKEN`：未认证限流 60 req/h，认证后 5000 req/h。

### 目标端

- **内部存储（默认）**：写入 `synced_records` 信封表，无需额外配置。
- **外部 PostgreSQL 表**：`target_connection_id` 指向一个 `postgres` 类型的连接。信封轨道（Mock/GitHub 源）需手动建表；镜像轨道（PostgreSQL 源）会根据源表结构**自动建同构表**。

## CDC 增量同步

与「游标轮询」的表级同步并行存在的第二条链路：**解码 WAL 而不是查询表**，毫秒级延迟、能捕获 DELETE、对源库零查询压力。

### 工作方式

1. **cdcreader**：对每个启用 CDC 的连接建 replication slot（`syncguard_slot_<id>`），解码 WAL，把变更事件**连同 LSN 断点在同一事务**写入中央库 `cdc_events`（去重键 `(connection_id, lsn)`）。
2. **cdc dispatcher**（cdcreader 进程内）：轮询 `cdc_events` 未分发事件，投到 RabbitMQ `syncguard.cdc` 交换机。
3. **cdcwriter**：消费 CDC 队列，按事件的 `connection_id` 解析该连接的目标库连接池，把 INSERT/UPDATE/DELETE 幂等应用到镜像表（INSERT 带 `ON CONFLICT DO NOTHING`）；目标表不存在时按事件携带的表结构元数据自动建同构表；失败就地退避重试保序，超限进 CDC 死信队列。

### 启用步骤

1. 在「数据源」页面创建/编辑一个 `postgres` 连接，勾选**启用 CDC** 并填写**镜像目标库连接串**（后端同样校验：仅 PostgreSQL 源可启用，target_url 必填）。
2. 在源库建好 `syncguard_pub` publication（在源库执行 `CREATE PUBLICATION syncguard_pub FOR ALL TABLES;`，自动覆盖源库所有表）。快照按该 publication 覆盖的表清单逐一搬存量，无需逐表配置。
3. 首次启用先做一次存量快照，再启动流式复制（命令见[命令行工具](#命令行工具)）。

> 设计细节（为什么 LSN 必须带 connection_id 维度、为什么事件表放中央库）见 [docs/CDC多租户路由设计.md](docs/CDC多租户路由设计.md)。

## 技术栈

- **语言**：Go 1.26
- **HTTP**：Gin
- **数据库**：PostgreSQL（pgx；CDC 用 pglogrepl 逻辑复制）
- **消息队列**：RabbitMQ（amqp091-go，主队列 + 分档延迟队列 + 死信队列 ×2 条链路）
- **缓存/协调**：Redis（go-redis，分布式锁 + 熔断状态）
- **鉴权**：JWT + bcrypt

## 快速开始

### 方式 A：Docker 一键部署（推荐）

前置：安装 Docker Desktop（Compose v2）+ 本机 Go 1.26+。

1. 本机交叉编译四个 Linux 二进制：

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/syncguard-server ./cmd/server
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/syncguard-worker ./cmd/worker
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/cdcreader ./cmd/cdcreader
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/cdcwriter ./cmd/cdcwriter
```

2. 构建镜像并启动：

```bash
docker compose -f deploy/docker-compose.yml up -d --build
```

启动 PostgreSQL / RabbitMQ / Redis → migrate 一次性容器按 schema_migrations 版本表应用迁移 → 启动 Server / Worker / cdcreader / cdcwriter。打开 `http://localhost:9090`：注册账号 → 创建数据源（Mock / GitHub / PostgreSQL）→ 创建任务（目标端默认内部存储，可选外部 PostgreSQL 表）→ 启动 → 查看同步结果 / 对账。

> 覆盖 JWT 密钥：`JWT_SECRET=你的密钥 docker compose -f deploy/docker-compose.yml up -d --build`。

### 方式 B：本地开发（go run）

前置：Go 1.26+。

1. 启动依赖（只起基础设施，不起应用容器，避免占用 9090）：

```bash
docker compose -f deploy/docker-compose.yml up -d postgres rabbitmq redis
```

2. 初始化数据库（首次）：`docker compose -f deploy/docker-compose.yml up -d migrate`（用 schema_migrations 版本表记录已执行，重复启动自动跳过）；或手动按顺序导入 `migrations/` 下的 SQL：

| 文件 | 作用 |
|------|------|
| `001_platform.sql` | 平台控制面全部 7 张表（users / connections / sync_jobs / synced_records / outbox / cdc_events / cdc_checkpoints）+ 索引，含 CDC 多租户字段 |

3. 配置环境变量：在项目根目录创建 `.env`（可参考下方[环境变量](#环境变量)表格）。

4. 启动服务：

```bash
go run ./cmd/server   # HTTP 服务，默认 :9090
go run ./cmd/worker   # 轮询 outbox + 消费队列执行同步
```

打开 `http://localhost:9090` 使用。

5. （可选）启用 CDC 链路：

```bash
go run ./cmd/cdcreader -snapshot   # 首次：存量快照（建槽钉切点 + 搬存量），完成后退出
go run ./cmd/cdcreader             # 流式：解码 WAL → cdc_events → MQ
go run ./cmd/cdcwriter             # 消费 CDC 队列，按连接路由到各自目标库
```

## 环境变量

| 变量 | 必需 | 进程 | 默认值 | 说明 |
|------|:---:|------|--------|------|
| `DATABASE_URL` | ✅ | 全部 | — | 中央库 PostgreSQL 连接串（业务表 + outbox + cdc_events） |
| `RABBITMQ_URL` | ✅ | 全部 | — | RabbitMQ AMQP 地址 |
| `JWT_SECRET` | ✅ | Server | — | JWT 签名密钥，空值拒绝启动 |
| `REDIS_ADDR` | ✅ | Worker | — | Redis 地址，如 `localhost:6379` |
| `REDIS_PASS` | ❌ | Worker | 空 | Redis 密码（无密码留空） |
| `PORT` | ❌ | Server | `9090` | HTTP 监听端口 |
| `GITHUB_TOKEN` | ❌ | Worker | 空 | GitHub 源认证 token（可选，认证后限流更高） |
| `SYNCGUARD_TEST_DB_URL` | ❌ | 测试 | 本地默认 PG | 覆盖集成测试的连接串 |

> CDC 链路不再需要额外的环境变量：源库/目标库连接串存在 `connections` 表的 `source_url` / `target_url` 里，cdcreader/cdcwriter 只连中央库。

## 命令行工具

| 命令 | 说明 |
|------|------|
| `docker compose -f deploy/docker-compose.yml run --rm cdcreader /app/cdcreader -snapshot` | 一次性存量快照：对每个启用 CDC 的连接建槽钉切点 + 搬存量到目标库后退出（单个连接失败跳过其余，最后汇总非零退出） |
| `docker compose -f deploy/docker-compose.yml run --rm cdcreader /app/cdcreader -connection <id>` | 只处理指定连接（0 = 全部启用 CDC 的连接），可与 `-snapshot` 组合 |
| `docker compose -f deploy/docker-compose.yml run --rm cdcwriter /app/cdcwriter -replay` | 一次性补投：把 CDC 死信队列的消息原样发回主队列后退出（配合 INSERT 幂等，可安全重放） |
| `docker compose -f deploy/docker-compose.yml run --rm worker /app/syncguard-worker -replay` | 一次性补投：把 jobs 死信队列的消息发回主队列后退出（Attempt 清零，给任务一次全新重试机会） |

## API 接口

| 方法 | 路径 | 鉴权 | 说明 |
|------|------|:---:|------|
| POST | `/api/register` | 公开 | 注册（注册即登录，返回 token） |
| POST | `/api/login` | 公开 | 登录（返回 token） |
| POST | `/api/logout` | JWT | 登出 |
| GET | `/api/connections` | JWT | 数据源列表 |
| POST | `/api/connections` | JWT | 创建数据源（支持 `cdc_enabled` + `target_url`） |
| PUT | `/api/connections/:id` | JWT | 更新数据源 |
| DELETE | `/api/connections/:id` | JWT | 删除数据源 |
| GET | `/api/jobs` | JWT | 任务列表 |
| POST | `/api/jobs` | JWT | 创建任务 |
| GET | `/api/jobs/:id` | JWT | 任务详情 |
| DELETE | `/api/jobs/:id` | JWT | 删除任务 |
| POST | `/api/jobs/:id/run` | JWT | 启动同步；`completed` 任务调用即「清断点全量重跑」 |
| POST | `/api/jobs/:id/reconcile` | JWT | 触发对账 |
| GET | `/api/records` | JWT | 同步记录列表（`limit` 上限 500） |

鉴权方式：`Authorization: Bearer <token>`（登录/注册接口返回的 JWT）。

## 目录结构

```
cmd/
  server/        HTTP 服务入口（装配 + 路由 + 优雅退出）
  worker/        消费进程入口（装配 + 消息循环 + jobs 死信补投）
  cdcreader/     CDC 读侧入口（每连接一个 reader：解码 WAL → cdc_events；含快照模式）
  cdcwriter/     CDC 写侧入口（消费 CDC 队列 → 按连接路由到各自目标库；含死信补投）
internal/
  model/         领域模型（Record / SyncJob / Connection / User / Diff / TableSchema / RawRow）
  engine/        同步引擎（Source/Destination 接口 + 信封分页循环 + 原表镜像循环）
  source/        数据源工厂 + mock / github / postgres 实现
  destination/   目标端工厂 + postgres 实现（可插拔目标端）
  repository/    数据访问层（连接池 + 各表 Store + outbox Store + 目标端适配器）
  mq/            RabbitMQ 拓扑（主队列/分档延迟/死信 × 两条链路）、Producer/Consumer、退避计算
  lock/          Redis 分布式锁 + 心跳续约
  circuitbreaker/ 三态熔断器 + 装饰器
  reconciliation/ 对账引擎
  cdc/           CDC 链路：reader（WAL 解码）/ dispatcher / consumer / applier（幂等应用）/ 快照 / 多租户连接解析
  redisclient/   Redis 客户端封装
  handler/       HTTP 处理器（鉴权 / 数据源 / 任务 / 记录 / 对账）
  middleware/    JWT 鉴权中间件
  worker/        Dispatcher（outbox 轮询投递）+ 同步编排（抢锁 → 续约 → 执行 → 分流）+ jobs 补投
migrations/      SQL 迁移文件（平台控制面表）
web/             前端静态页面（登录 / 注册 / 数据源 / 任务 / 结果）
deploy/          docker-compose 与部署说明
docs/            设计文档（CDC 多租户路由、RAG 管道 CDC 设计、改进方案、代码审查报告）
```

## 测试

```bash
go test ./...
```

覆盖：

| 包 | 验证内容 |
|----|---------|
| engine | 完整同步、断点续传、幂等、失败标记；镜像循环（建表→列对列→断点） |
| lock | 抢锁、互斥、释放后可再抢 |
| circuitbreaker | 连续失败熔断、冷却后放探针、探针成功复原 |
| mq | 退避延迟、outbox payload 序列化 |
| source/mock | 分页正确性 |
| source/postgres | 表名解析、行映射；分页 Fetch + 表结构内省/原生行分页（集成，需本地 PG） |
| destination/postgres | 表名白名单、空批次；幂等 upsert + 建表/列对列写入类型保真（集成，需本地 PG） |
| middleware | JWT 签发/校验、鉴权中间件四条路径 |
| reconciliation | 对账四类差异；镜像表对账 |
| worker | outbox 分发（立即 / 延迟 / 发送失败重试 / 坏消息跳过） |
| repository | outbox 全链路、synced_records upsert 与隔离（集成，需本地 PG） |

集成测试连接本地 PG（连接串可用 `SYNCGUARD_TEST_DB_URL` 覆盖），连不上自动跳过；`docker compose up -d` 启动依赖后跑 `go test ./...` 可全量验证，无需手动建表核对。

## 常见问题

<details>
<summary><b>启动报「JWT_SECRET 不能为空」？</b></summary>

`JWT_SECRET` 是必填项，需在 `.env` 中配置任意非空字符串。项目刻意不让服务在空密钥下启动，以避免"无密钥签发 token"的隐患。
</details>

<details>
<summary><b>Worker 报「Redis 连接失败」？</b></summary>

确认 `docker compose up -d` 已启动 Redis，且 `.env` 中的 `REDIS_ADDR` / `REDIS_PASS` 与实际一致（默认无密码，`REDIS_PASS` 留空）。
</details>

<details>
<summary><b>同步 GitHub 源时总是限流（403/429）？</b></summary>

未认证请求仅 60 req/h。配置 `GITHUB_TOKEN` 后可达 5000 req/h。
</details>

<details>
<summary><b>cdcreader 报「创建 CDC Reader 失败，跳过该连接」？</b></summary>

依次检查：该连接的 `source_url` 是否可达；源库 `wal_level=logical`；源库是否已建好 `syncguard_pub` publication（在源库执行 `CREATE PUBLICATION syncguard_pub FOR ALL TABLES;`）。单个连接失败不影响其余连接。
</details>

<details>
<summary><b>cdcreader 异常退出后源库磁盘暴涨？</b></summary>

replication slot 会钉住 WAL。cdcreader 异常时进程会整体退出（交 supervisor 重启）；若长期停用某连接的 CDC，记得在源库 `SELECT pg_drop_replication_slot('syncguard_slot_<id>')` 清理槽位。
</details>

<details>
<summary><b>集成测试被跳过（skip）？</b></summary>

集成测试需要本地 PostgreSQL，连不上时自动跳过。先 `docker compose up -d` 启动依赖，再跑 `go test ./...` 即可全量执行。
</details>

## Roadmap

- [x] PostgreSQL 数据源（表级同步，`表名[:主键列]` 语法）
- [x] 目标端抽象（默认内部存储 / 外部 PostgreSQL 表）
- [x] Outbox 模式（写库与发消息同事务，保证消息不丢）
- [x] 目标表自动建表（镜像轨道：检测源表结构 → 目标库一键建同构表；信封轨道外部表仍需手动建）
- [x] 两种轨道：源是数据库就镜像成原表结构（列对列、类型保真）；源是 API / 文档使用通用信封
- [x] 部署上云（Dockerfile + 容器化编排）
- [x] 真 CDC 增量同步（逻辑复制 + LSN 断点 + 两段式 outbox + 多租户路由 + 死信补投）
- [ ] 更多数据源 / 目标端（MySQL、Kafka 等）
- [x] CDC 表名可配置（publication 即表清单，`FOR ALL TABLES` 自动抓取；快照按 publication 枚举、增量自动建缺失目标表）
- [ ] Prometheus 指标与可观测性

## License

**个人学习项目**
