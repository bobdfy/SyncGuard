#!/bin/sh
# SyncGuard 数据库迁移守卫脚本（一次性容器内执行）
#   1) 等 postgres 就绪（pg_isready）
#   2) 逐表判断：表已存在则跳过对应迁移，只补缺失的（既不重复执行报错，也不漏掉部分迁移）
#   3) 新增迁移文件时，在下方追加一行 apply_if_missing <该迁移建的表> <文件.sql>
set -eu

export PGPASSWORD="${PGPASSWORD:-syncguard}"
: "${PGHOST:=postgres}" "${PGUSER:=postgres}" "${PGDATABASE:=postgres}"

PSQL="psql -h $PGHOST -U $PGUSER -d $PGDATABASE"

echo "[migrate] 等待 PostgreSQL 就绪..."
until pg_isready -h "$PGHOST" -p "${PGPORT:-5432}" -U "$PGUSER" -d "$PGDATABASE" >/dev/null 2>&1; do
  sleep 2
done
echo "[migrate] PostgreSQL 已就绪"

# 表存在 → 跳过；缺失 → 应用对应迁移文件
apply_if_missing() {
  table="$1"
  file="$2"
  if [ "$($PSQL -tAc "SELECT to_regclass('public.$table')")" = "$table" ]; then
    echo "[migrate] 表 $table 已存在，跳过 $file"
  else
    echo "[migrate] 应用 $file（缺失表 $table）"
    $PSQL -v ON_ERROR_STOP=1 -f "/migrations/$file"
  fi
}

apply_if_missing users 001_init.sql
apply_if_missing outbox 002_outbox.sql
apply_if_missing documents 003_cdc.sql
apply_if_missing cdc_events 004_cdc_events.sql
# 新增迁移：apply_if_missing <该迁移建的表> <新文件.sql>

echo "[migrate] 迁移检查完成"
