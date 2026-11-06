#!/usr/bin/env bash
# 由官方镜像 source，使用初始化 socket 创建 UC 账号并授予此库的权限。
set -e
if [[ ! "$SLG_UC_DB_PASSWORD" =~ ^[A-Za-z0-9_-]{24,128}$ ]]; then
  printf '%s\n' 'UC password must be 24-128 letters, digits, underscores or hyphens.' >&2
  return 1
fi
docker_process_sql <<SQL
CREATE USER IF NOT EXISTS 'slg_uc'@'%' IDENTIFIED BY '${SLG_UC_DB_PASSWORD}';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, INDEX, REFERENCES ON slg_uc.* TO 'slg_uc'@'%';
SQL
