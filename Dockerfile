# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS certs

# 阶段 2：运行时 —— 极简 alpine，只放二进制 + 静态页 + CA 证书
FROM alpine:3.22

# GitHub API 等 HTTPS 出站需要的根证书（从阶段 1 拷贝，避免 apk 联网安装）
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

# busybox 自带 addgroup/adduser，无需联网安装
RUN addgroup -S app && adduser -S -G app app

WORKDIR /app
COPY bin/syncguard-server bin/syncguard-worker bin/cdcreader bin/cdcwriter ./
# 静态文件走磁盘相对路径 ./web（server 代码 r.Static("/static", "./web")），WORKDIR 必须与它对齐
COPY web/ ./web/

# 最小权限：非 root 运行
USER app

EXPOSE 9090

# 默认启动 server；worker/cdcreader/cdcwriter 容器在 compose 里用 command 覆盖入口
CMD ["/app/syncguard-server"]
