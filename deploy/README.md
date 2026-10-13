# 部署（容器化）

## 构建 + 部署

> 说明：本机 Docker 容器网络出站被代理/VPN 干扰，构建阶段无法联网，因此镜像采用「宿主机交叉编译 + 打进镜像」的方式：二进制在本机编译，Dockerfile 只做拷贝，构建过程零联网、可复现。

**第 1 步：本机交叉编译**（Windows 下产出 Linux 静态二进制，在仓库根目录执行）：

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/syncguard-server ./cmd/server
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/syncguard-worker ./cmd/worker
```

**第 2 步：构建镜像并启动**：

```bash
docker compose -f deploy/docker-compose.yml up -d --build
```

启动 PostgreSQL / RabbitMQ / Redis → 自动执行数据库迁移（带数据守卫）→ 启动 Server（:9090）与 Worker。打开 http://localhost:9090 即可使用。

**覆盖 JWT 密钥**（三选一，优先级从高到低）：

```bash
# 方式 1：shell 环境变量（临时）
JWT_SECRET=my-secret docker compose -f deploy/docker-compose.yml up -d

# 方式 2：deploy/.env（持久；compose 项目目录是 deploy/，不会读仓库根目录的 .env）
echo "JWT_SECRET=my-secret" > deploy/.env

# 方式 3：直接改 compose 里 server 服务的 JWT_SECRET 默认值（不推荐提交真实密钥）
```

## 服务拓扑

| 服务 | 容器名 | 宿主机端口 | 说明 |
|------|--------|-----------|------|
| PostgreSQL | syncguard-postgres | 5432 | 数据卷 `syncguard_postgres_data`（勿改挂载路径 `/var/lib/postgresql`） |
| RabbitMQ | syncguard-rabbitmq | 5672 / 15672 | 管理界面 http://localhost:15672（syncguard / syncguard） |
| Redis | syncguard-redis | 6379 | 分布式锁 / 熔断状态（可重建，无持久业务数据） |
| migrate | syncguard-migrate | — | 一次性迁移容器，跑完即退出（Exited 0） |
| Server | syncguard-server | 9090 | Gin API + 前端静态页 |
| Worker | syncguard-worker | — | 消费队列执行同步 |

## 迁移行为（数据守卫）

`migrate` 是 `restart: "no"` 的一次性容器，逻辑：等 postgres 就绪 → 查 `users` 表 → **存在则跳过**（保护现有数据不被重建）→ 不存在则按文件名顺序应用 `migrations/*.sql`。

- 本机现有 `syncguard_postgres_data` 卷已初始化 → 走「跳过」分支，日志可见 `[migrate] users 表已存在，跳过迁移`；
- 全新环境（无卷）→ 自动建齐全部表；
- 后续新增迁移文件（如 `003_xxx.sql`）不会自动应用（守卫是粗粒度的），手动执行：

  ```bash
  docker exec -i syncguard-postgres psql -U postgres -d postgres < migrations/003_xxx.sql
  ```

- 彻底重置（删除三个数据卷，不可恢复）：

  ```bash
  docker compose -f deploy/docker-compose.yml down -v
  ```

## 常用操作

```bash
CFG="-f deploy/docker-compose.yml"   # 在仓库根目录执行

docker compose $CFG ps                      # 状态（migrate 应为 Exited (0)）
docker compose $CFG logs -f server worker   # 跟踪应用日志
docker compose $CFG logs migrate            # 迁移日志（看守卫走了哪个分支）
docker compose $CFG restart server          # 重启单个服务
docker compose $CFG up -d --build           # 代码更新后重建镜像并重启
docker compose $CFG down                    # 停止全部（保留数据卷）
docker compose $CFG down -v                 # 停止并删除数据卷（慎用，数据不可恢复）
```

## 本地开发模式（go run）

只起基础设施、不起应用容器（避免占用 9090 与本地 `go run` 冲突）：

```bash
docker compose -f deploy/docker-compose.yml up -d postgres rabbitmq redis
```

之后按根 README「快速开始 - 方式 B」跑 `go run ./cmd/server` / `go run ./cmd/worker`。首次初始化数据库也可直接复用迁移容器：

```bash
docker compose -f deploy/docker-compose.yml up -d migrate
```

## 版本固定说明

- **PostgreSQL**：数据卷由某个 major 版本初始化，若 tag 用 `latest`，镜像升级 major 后旧卷会被拒绝启动。compose 里默认 `postgres:${POSTGRES_VERSION:-18}`。探测现有卷版本：

  ```bash
  # 容器运行时
  docker exec syncguard-postgres cat /var/lib/postgresql/data/PG_VERSION
  # 或容器未运行时直接读卷
  docker run --rm -v syncguard_postgres_data:/data alpine cat /data/data/PG_VERSION
  ```

  若输出不是 `18`，用 `POSTGRES_VERSION=<该版本> docker compose ... up -d` 覆盖，或直接改 compose 默认值。
- **RabbitMQ** 已固定 `4.3-management`；**Redis** 固定 `8-alpine`（其数据仅为锁/熔断状态，重建无损失）。

## 本机网络适配说明

- 本机 Docker 容器桥接网络出站被代理/VPN 干扰，构建容器无法联网，故镜像采用「本机交叉编译 + 拷贝进镜像」，构建过程不联网；
- 首次构建需拉 `golang:1.26-alpine`（借用其 CA 证书）与 `alpine`（约百 MB），这两次拉取走 Docker 守护进程、不受上述问题影响；如 Docker Hub 慢，可在 Docker Desktop 设置里配置 registry-mirrors 镜像加速器；
- 在容器网络正常的机器上，可改用标准多阶段构建（builder 阶段 `go mod download` 编译源码），得到同样的产物；
- 容器内未装 tzdata，日志时间显示 UTC；如需本地时区，可在 `cmd/server`、`cmd/worker` 入口加 `import _ "time/tzdata"` 内嵌时区数据。
