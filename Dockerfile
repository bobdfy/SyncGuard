# syntax=docker/dockerfile:1
# ============================================================
# SyncGuard 镜像（本机受限网络适配版）
#
# 背景：本机 Docker 容器网络出站被代理/VPN 干扰，构建阶段无法联网，
# 因此改为「宿主机交叉编译 + 打进镜像」：Go 二进制在本机（Windows）交叉编译为
# linux/amd64 静态产物，Dockerfile 只做拷贝，构建过程零联网、可复现。
#
# 构建镜像前先在本机执行（见 deploy/README.md「构建」）：
#   GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/syncguard-server ./cmd/server
#   GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/syncguard-worker ./cmd/worker
# ============================================================

# 阶段 1：仅借用 golang 基础镜像自带的 CA 根证书（本机已缓存该镜像，无需联网拉取）
FROM golang:1.26-alpine AS certs

# 阶段 2：运行时 —— 极简 alpine，只放二进制 + 静态页 + CA 证书
FROM alpine:3.22

# GitHub API 等 HTTPS 出站需要的根证书（从阶段 1 拷贝，避免 apk 联网安装）
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

# busybox 自带 addgroup/adduser，无需联网安装
RUN addgroup -S app && adduser -S -G app app

WORKDIR /app
COPY bin/syncguard-server bin/syncguard-worker ./
# 静态文件走磁盘相对路径 ./web（server 代码 r.Static("/static", "./web")），WORKDIR 必须与它对齐
COPY web/ ./web/

# 最小权限：非 root 运行
USER app

EXPOSE 9090

# 默认启动 server；worker 容器在 compose 里用 command 覆盖为 /app/syncguard-worker
CMD ["/app/syncguard-server"]
