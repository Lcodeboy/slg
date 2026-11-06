#!/usr/bin/env python3
"""Manage this project's isolated MySQL development container without deleting data."""
# 管理独立开发 MySQL 容器；配置与数据卷长期保留，不提供隐式删库或重置操作。
import argparse
import fcntl
import os
from pathlib import Path
import re
import secrets
import shlex
import subprocess
import sys
import tempfile

from local_services import ROOT, read_env


def projection_url(port):
    return (f"jdbc:mysql://127.0.0.1:{port}/slg_projection?sslMode=DISABLED&allowPublicKeyRetrieval=true"
            "&connectionTimeZone=UTC&connectTimeout=2000&socketTimeout=5000")


def validate_development_projection(env, port):
    if env.get("SLG_PROJECTION_DB_USER") != "slg_projection":
        raise ValueError("Development initialization expects SLG_PROJECTION_DB_USER=slg_projection")
    password = env.get("SLG_PROJECTION_DB_PASSWORD", "")
    if not re.fullmatch(r"[A-Za-z0-9_-]{24,128}", password) or password.startswith("GENERATE_"):
        raise ValueError("SLG_PROJECTION_DB_PASSWORD must be 24-128 letters, digits, underscores or hyphens")
    url = env.get("SLG_PROJECTION_JDBC_URL", "")
    if not url.startswith(f"jdbc:mysql://127.0.0.1:{port}/slg_projection?") or "\n" in url or "\r" in url:
        raise ValueError("Update SLG_PROJECTION_JDBC_URL when changing SLG_MYSQL_PORT")


def prepare(path):
    """为新开发环境生成私有配置，旧环境只补缺失字段。启动、补建并发时必须选中同一个密码。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(path.name + ".prepare.lock")
    # 独立锁文件在原子替换后仍是同一个 inode。已锁住的旧配置文件本身不能充当这把锁。
    with os.fdopen(os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600), "r+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if path.exists():
            configured = read_env(path)
            port = int(configured.get("SLG_MYSQL_PORT", "13306"))
            additions = {
                "SLG_PROJECTION_DB_USER": "slg_projection",
                "SLG_PROJECTION_DB_PASSWORD": secrets.token_urlsafe(36),
                "SLG_PROJECTION_JDBC_URL": projection_url(port),
            }
            missing = {key: os.environ.get(key, value) for key, value in additions.items() if key not in configured}
            if not missing:
                return
            # 保留原内容及已有密码；显式空值由 configuration 报错，绝不擅自轮换已部署凭证。
            validate_development_projection({**configured, **missing}, port)
            text = path.read_bytes() + b"\n" + "".join(
                f"{key}={shlex.quote(value)}\n" for key, value in missing.items()).encode()
        else:
            template = (ROOT / ".env.mysql.example").read_text()
            for marker in ("GENERATE_ROOT_PASSWORD", "GENERATE_UC_PASSWORD", "GENERATE_PROJECTION_PASSWORD"):
                template = template.replace(marker, secrets.token_urlsafe(36))
            text = template.encode()
        # 先完整写入 0600 临时文件再替换；磁盘写满或进程中断不能留下半截密码和不可读的旧文件。
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".mysql-prepare-", delete=False) as temp:
            try:
                temp.write(text)
                temp.flush()
                os.fsync(temp.fileno())
                os.replace(temp.name, path)
            finally:
                Path(temp.name).unlink(missing_ok=True)
        print("Prepared private MySQL configuration: " + str(path), flush=True)


# 环境变量覆盖文件配置；校验初始化账号和 JDBC 端口一致，防止应用误连另一实例。
def configuration(path, *, need_projection=True):
    if not path.is_file():
        raise ValueError("Configuration missing; run ./scripts/mysql-local.sh prepare first")
    env = {**read_env(path), **os.environ}
    port = int(env.get("SLG_MYSQL_PORT", "13306"))
    if not 1 <= port <= 65535:
        raise ValueError("SLG_MYSQL_PORT must be within 1..65535")
    # 密码字符集与容器初始化脚本一致，避免把引号或 SQL 控制字符带入初始化语句。
    for key in ("SLG_MYSQL_ROOT_PASSWORD", "SLG_UC_DB_PASSWORD"):
        value = env.get(key, "")
        if not re.fullmatch(r"[A-Za-z0-9_-]{24,128}", value) or value.startswith("GENERATE_"):
            raise ValueError(key + " must be 24-128 letters, digits, underscores or hyphens")
    if env.get("SLG_UC_DB_USER") != "slg_uc":
        raise ValueError("Development initialization expects SLG_UC_DB_USER=slg_uc")
    if not env.get("SLG_UC_JDBC_URL", "").startswith(f"jdbc:mysql://127.0.0.1:{port}/slg_uc?"):
        raise ValueError("Update SLG_UC_JDBC_URL when changing SLG_MYSQL_PORT")
    if need_projection:
        validate_development_projection(env, port)
    return env


def provision(command, env):
    """在已运行的开发容器内用管理员连接补建。JDBC 运行账号永远不负责建表或迁移。"""
    # 登记密码只通过进程环境传给 docker/mysql，不进入命令行或终端输出。把版本化 SQL 送入
    # stdin，已有容器即使只挂载了旧版 init 目录，也能执行与新卷完全相同的 DDL。
    init = (ROOT / "scripts/mysql/init/02-projection.sh").read_text()
    apply_script = ('docker_process_sql() { mysql --protocol=SOCKET --user=root "$@"; }; '
                    'SLG_PROJECTION_SCHEMA_FILE=/dev/stdin; provision_projection() {\n'
                    + init + '\n}\nprovision_projection')
    subprocess.run([*command, "exec", "-T", "-e", "MYSQL_PWD", "-e", "SLG_PROJECTION_DB_PASSWORD",
                    "mysql", "bash", "-ec", apply_script], input=projection_schema_sql().encode(), cwd=ROOT,
                   env={**env, "MYSQL_PWD": env["SLG_MYSQL_ROOT_PASSWORD"]}, check=True)
    # CREATE USER IF NOT EXISTS 不改已有用户密码。再以运行身份连一次，捕获旧卷密码不匹配，
    # 不能出现“补建成功”但游戏进程始终无法连接的假成功。
    subprocess.run([*command, "exec", "-T", "-e", "MYSQL_PWD", "mysql", "mysql", "--protocol=TCP",
                    "--host=127.0.0.1", "--user=slg_projection", "--database=slg_projection",
                    "--execute=SELECT 1 FROM projection_checkpoint LIMIT 0; SELECT account_id,founding_realm_id,current_realm_id FROM projection_player LIMIT 0; "
                    "SELECT occupant_player_id FROM projection_resource_site LIMIT 0; SELECT version FROM projection_schema_migration WHERE version=3"], cwd=ROOT,
                   env={**env, "MYSQL_PWD": env["SLG_PROJECTION_DB_PASSWORD"]}, check=True)
    print("Projection schema and runtime account verified.", flush=True)


def projection_schema_sql():
    """开发初始化与隔离故障验收采用同一组发布 SQL；缺任一已登记迁移都应在切换读方前失败。"""
    directory = ROOT / "scripts/mysql/schema"
    names = ("001-realm-projection.sql", "003-role-identity-projections.sql")
    return "\n".join((directory / name).read_text() for name in names)


# 只操作固定 Compose 项目中的 mysql 服务；prepare 不启动容器，stop 不删除卷。
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "start", "provision", "stop", "status"))
    parser.add_argument("--env-file", type=Path, default=ROOT / "data/mysql.env")
    args = parser.parse_args()
    path = args.env_file.resolve()
    if args.action in ("prepare", "start", "provision"):
        prepare(path)
    env = configuration(path, need_projection=args.action not in ("stop", "status"))
    if args.action == "prepare":
        print("Configuration ready; no container was started", flush=True)
        return 0
    command = ["docker", "compose", "--project-name", "slg-mysql-dev", "--env-file", str(path),
               "--file", str(ROOT / "compose.mysql.yaml")]
    if args.action == "provision":
        provision(command, env)
        return 0
    # 启动必须等待 Compose 健康检查成功，不能只以容器进程已创建作为数据库就绪。
    command += {"start": ["up", "--detach", "--wait", "--wait-timeout", "180", "mysql"],
                "stop": ["stop", "mysql"], "status": ["ps", "mysql"]}[args.action]
    subprocess.run(command, cwd=ROOT, env=env, check=True)
    if args.action == "start":
        print("MySQL ready at 127.0.0.1:" + env.get("SLG_MYSQL_PORT", "13306"), flush=True)
        config_arg = "" if path == ROOT / "data/mysql.env" else " --env-file " + shlex.quote(str(path))
        print("On a previously initialized volume, run ./scripts/mysql-local.sh provision" + config_arg, flush=True)
        print("Application startup: ./scripts/start-local.sh" + config_arg, flush=True)
    elif args.action == "stop":
        print("MySQL stopped; its dedicated volume and credentials are retained", flush=True)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print("Error: " + str(error), file=sys.stderr)
        sys.exit(1)
