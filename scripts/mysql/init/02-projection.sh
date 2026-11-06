#!/usr/bin/env bash
# 新卷由 MySQL 镜像 source；已有卷由 mysql-local.sh provision 使用相同的管理员 SQL 入口。
# 两条路径都读取仓库里的版本化 DDL，避免开发卷和恢复验收因使用不同表结构而得到不同结果。
set -e
if [[ ! "${SLG_PROJECTION_DB_PASSWORD:-}" =~ ^[A-Za-z0-9_-]{24,128}$ ]]; then
  printf '%s\n' 'Invalid SLG_PROJECTION_DB_PASSWORD for the development projection account.' >&2
  return 1
fi

# 建库、建表属于管理员职责。CREATE IF NOT EXISTS 允许中断后重跑；已存在账号不会被改密码。
docker_process_sql <<SQL
CREATE DATABASE IF NOT EXISTS slg_projection DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE USER IF NOT EXISTS 'slg_projection'@'%' IDENTIFIED BY '${SLG_PROJECTION_DB_PASSWORD}';
SQL

# IF NOT EXISTS 可能遇到其他用途的同名用户。在授予权限前先核实身份和已有授权；密码不符或
# 已带额外权限时中止，由管理员处理，避免向错误身份开放投影库，也避免静默撤销其既有授权。
projection_socket=()
if [[ -n "${SOCKET:-}" ]]; then projection_socket=("--socket=$SOCKET"); fi
projection_grants=$(MYSQL_PWD="$SLG_PROJECTION_DB_PASSWORD" mysql --protocol=SOCKET "${projection_socket[@]}" \
  --user=slg_projection --batch --skip-column-names --execute='SELECT CURRENT_USER(); SHOW GRANTS;') || return 1
while IFS= read -r grant_line; do
  case "$grant_line" in
    'slg_projection@%'|'GRANT USAGE ON *.* TO `slg_projection`@`%`'|\
    'GRANT SELECT, INSERT ON `slg_projection`.`projection_event` TO `slg_projection`@`%`'|\
    'GRANT SELECT, INSERT, UPDATE ON `slg_projection`.`projection_object` TO `slg_projection`@`%`'|\
    'GRANT SELECT ON `slg_projection`.`projection_schema_migration` TO `slg_projection`@`%`'|\
    'GRANT SELECT ON `slg_projection`.`projection_player` TO `slg_projection`@`%`'|\
    'GRANT SELECT ON `slg_projection`.`projection_resource_site` TO `slg_projection`@`%`'|\
    'GRANT SELECT, INSERT, UPDATE ON `slg_projection`.`projection_checkpoint` TO `slg_projection`@`%`') ;;
    *) printf '%s\n' 'Existing projection account has unexpected identity or grants; inspect before provisioning.' >&2; return 1 ;;
  esac
done <<< "$projection_grants"
if [[ -n "${SLG_PROJECTION_SCHEMA_FILE:-}" ]]; then
  # 显式 provision 把本次发布的全部迁移按顺序送到 stdin，适用于已有数据卷。
  docker_process_sql --database=slg_projection < "$SLG_PROJECTION_SCHEMA_FILE" || return 1
else
  # 002 保存旧 UUID 库的定义；新身份基线仅安装 001 和 003。
  for migration in /opt/slg-schema/001-realm-projection.sql /opt/slg-schema/003-role-identity-projections.sql; do
    docker_process_sql --database=slg_projection < "$migration" || return 1
  done
fi

# CREATE TABLE IF NOT EXISTS 遇到同名老表不会更换引擎。业务表和迁移标记表都必须是 InnoDB；否则
# ROLLBACK 可能只撤销游标、不撤销流水，导致一次失败的投影留下部分可见的数据。
projection_table_count=$(docker_process_sql --batch --skip-column-names --execute="
  SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='slg_projection'
    AND table_name IN ('projection_checkpoint','projection_event','projection_object',
                       'projection_player','projection_resource_site','projection_schema_migration') AND engine='InnoDB'") || return 1
if [[ "$projection_table_count" != "6" ]]; then
  printf '%s\n' 'Expected six InnoDB projection tables; inspect the existing schema before granting access.' >&2
  return 1
fi

# 流水只有追加和核对读取。对象快照与连续游标另需 UPDATE；运行账号不能删数据、建表或访问 UC。
docker_process_sql <<'SQL'
GRANT SELECT, INSERT ON slg_projection.projection_event TO 'slg_projection'@'%';
GRANT SELECT, INSERT, UPDATE ON slg_projection.projection_object TO 'slg_projection'@'%';
GRANT SELECT, INSERT, UPDATE ON slg_projection.projection_checkpoint TO 'slg_projection'@'%';
GRANT SELECT ON slg_projection.projection_schema_migration TO 'slg_projection'@'%';
GRANT SELECT ON slg_projection.projection_player TO 'slg_projection'@'%';
GRANT SELECT ON slg_projection.projection_resource_site TO 'slg_projection'@'%';
SQL
