#!/bin/sh
# SyncGuard 数据库迁移守卫脚本（一次性容器内执行）
#   1) 等 postgres 就绪（pg_isready）
#   2) 用 schema_migrations 版本表记录已执行文件，按文件名序 apply 每个 *.sql 一次
#      （替代旧的「表存在就跳过」——那种判断遇到给已存在表加列/改键的迁移会漏执行）
#   3) 新增迁移：只需在 migrations/ 下按序号加一个新 .sql 文件，无需改本脚本
set -eu

export PGPASSWORD="${PGPASSWORD:-syncguard}"
: "${PGHOST:=postgres}" "${PGUSER:=postgres}" "${PGDATABASE:=postgres}"

PSQL="psql -v ON_ERROR_STOP=1 -h $PGHOST -U $PGUSER -d $PGDATABASE"

echo "[migrate] 等待 PostgreSQL 就绪..."
until pg_isready -h "$PGHOST" -p "${PGPORT:-5432}" -U "$PGUSER" -d "$PGDATABASE" >/dev/null 2>&1; do
  sleep 2
done
echo "[migrate] PostgreSQL 已就绪"

# 版本表：记录已执行的迁移文件名
$PSQL -c "CREATE TABLE IF NOT EXISTS schema_migrations (
    filename   TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);"

apply() {
  file="$1"
  if [ "$($PSQL -tAc "SELECT 1 FROM schema_migrations WHERE filename = '$file'")" = "1" ]; then
    echo "[migrate] 已应用 $file，跳过"
  else
    echo "[migrate] 应用 $file"
    $PSQL -f "/migrations/$file"
    $PSQL -c "INSERT INTO schema_migrations (filename) VALUES ('$file');"
  fi
}

for f in /migrations/*.sql; do
  apply "$(basename "$f")"
done

echo "[migrate] 迁移检查完成"
