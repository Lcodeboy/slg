#!/usr/bin/env python3
"""使用一次性 MySQL 运行集成测试及真实停机恢复验收，不读取开发库的地址和凭据。"""
import argparse
import json
import os
from pathlib import Path
import re
import secrets
import socket
import subprocess
import sys
import tempfile
import time

from mysql_local import projection_schema_sql

ROOT = Path(__file__).resolve().parent.parent
PROJECTION_SCHEMA = "slg_projection_it"
PROJECTION_USER = "slg_projection_it"


def docker(arguments, **kwargs):
    """通过 argv 调 Docker；不把密码写入命令行，不向日志输出容器环境。"""
    return subprocess.run(["docker", *arguments], text=True, check=True,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs)


def owned_id(name, expected=None):
    """用一次 inspect 同时取得 ID 和标签。后续按不可变 ID 操作，避免同名替换导致误停或误删。"""
    if not re.fullmatch(r"slg-mysql-it-[0-9a-f]{12}", name):
        raise RuntimeError("Refused to operate on a container outside this integration run")
    result = subprocess.run(["docker", "inspect", "--format", "{{.Id}} {{json .Config.Labels}}", name],
                            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15)
    if result.returncode:
        if "No such object" in result.stderr or "No such container" in result.stderr:
            return None
        raise RuntimeError("Could not inspect the temporary test container")
    identity, labels_json = result.stdout.strip().split(" ", 1)
    labels = json.loads(labels_json) or {}
    if (not re.fullmatch(r"[0-9a-f]{64}", identity) or expected is not None and identity != expected
            or labels.get("com.slg.test-run") != name or labels.get("com.slg.purpose") != "mysql-integration-test"):
        raise RuntimeError("Container identity or ownership labels do not match this test run")
    return identity


def cleanup(name, expected=None):
    """数据目录使用 Docker 匿名卷；rm --volumes 只回收归属核验通过的本次容器及匿名卷。"""
    identity = owned_id(name, expected)
    if identity is None:
        return False
    docker(["rm", "--force", "--volumes", identity], timeout=30)
    return True


def change_mysql_state(name, identity, action):
    """故障注入必须对创建时取得的同一 ID 作业，不能操作调用者传入的开发容器或 Compose 服务。"""
    if action not in ("start", "stop") or owned_id(name, identity) != identity:
        raise RuntimeError("Refused to change the state of a container not owned by this test run")
    docker([action, identity], timeout=60)


def mysql(identity, db_env, statement, database=None):
    """用 stdin 传 SQL；batch/raw 避免把 JSON 编码两次，方便逐字段核对投影对象。"""
    args = ["exec", "--interactive", "--env", "MYSQL_PWD", identity, "mysql", "--protocol=TCP",
            "--host=127.0.0.1", "--user=root", "--batch", "--raw", "--skip-column-names"]
    if database:
        args.append("--database=" + database)
    return docker(args, env=db_env, input=statement, timeout=15).stdout.rstrip("\n")


def wait_mysql(identity, db_env):
    """镜像初始化阶段临时 mysqld 仅接受 Unix socket，需等正式 TCP 服务完成恢复后才放行。"""
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            return mysql(identity, db_env, "SELECT VERSION()")
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            time.sleep(1)
    raise RuntimeError("The isolated MySQL server did not become ready within 120 seconds")


def run(image, projection_only=False, unity_probe=None):
    name = "slg-mysql-it-" + secrets.token_hex(6)
    password = secrets.token_hex(32)
    projection_password = secrets.token_hex(32)
    db_env = {**os.environ, "MYSQL_PWD": password}
    # 显式取得空闲端口并固定映射。Docker 在 stop/start 后可能重新分配自动端口，GAME 必须能通过原地址重连。
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        port = reserved.getsockname()[1]
    identity = None
    attempted = False
    with tempfile.TemporaryDirectory(prefix="slg-mysql-it-") as directory:
        env_file = Path(directory) / "mysql.env"
        with os.fdopen(os.open(env_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), "w") as stream:
            stream.write("MYSQL_ROOT_PASSWORD=" + password + "\nMYSQL_ROOT_HOST=%\n")
        try:
            print("Starting an isolated MySQL container on a random loopback port and private temporary volume.", flush=True)
            # 调用超时也可能已创建容器。保留匿名数据卷才能真正停止 MySQL 后从原游标恢复。
            attempted = True
            identity = docker(["run", "--detach", "--name", name,
                               "--label", "com.slg.purpose=mysql-integration-test",
                               "--label", "com.slg.test-run=" + name, "--env-file", str(env_file),
                               "--publish", f"127.0.0.1:{port}:3306", "--mount", "type=volume,destination=/var/lib/mysql",
                               image, "--character-set-server=utf8mb4", "--collation-server=utf8mb4_0900_ai_ci"],
                              timeout=240).stdout.strip()
            if owned_id(name, identity) != identity:
                raise RuntimeError("Could not confirm the created test container identity")
            version = wait_mysql(identity, db_env)
            print("MySQL " + version + " is ready; initializing isolated UC and projection databases.", flush=True)
            mysql(identity, db_env, f"""
                CREATE DATABASE slg_uc_it CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
                CREATE DATABASE {PROJECTION_SCHEMA} CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
                CREATE USER '{PROJECTION_USER}'@'%' IDENTIFIED BY '{projection_password}';
            """)
            mysql(identity, db_env, projection_schema_sql(), PROJECTION_SCHEMA)
            # 真实 GAME 只追加事实、更新对象和游标。这里也用逐表授权，演练能发现误用 UPDATE 归档历史等越权 SQL。
            mysql(identity, db_env, f"""
                GRANT SELECT, INSERT ON {PROJECTION_SCHEMA}.projection_event TO '{PROJECTION_USER}'@'%';
                GRANT SELECT, INSERT, UPDATE ON {PROJECTION_SCHEMA}.projection_object TO '{PROJECTION_USER}'@'%';
                GRANT SELECT, INSERT, UPDATE ON {PROJECTION_SCHEMA}.projection_checkpoint TO '{PROJECTION_USER}'@'%';
                GRANT SELECT ON {PROJECTION_SCHEMA}.projection_schema_migration TO '{PROJECTION_USER}'@'%';
                GRANT SELECT ON {PROJECTION_SCHEMA}.projection_player TO '{PROJECTION_USER}'@'%';
                GRANT SELECT ON {PROJECTION_SCHEMA}.projection_resource_site TO '{PROJECTION_USER}'@'%';
            """)
            options = "?sslMode=DISABLED&allowPublicKeyRetrieval=true&connectionTimeZone=UTC&connectTimeout=2000&socketTimeout=5000"
            jdbc = f"jdbc:mysql://127.0.0.1:{port}/"
            if not projection_only:
                env = {**os.environ, "SLG_MYSQL_IT_UC_URL": jdbc + "slg_uc_it" + options,
                       "SLG_MYSQL_IT_USER": "root", "SLG_MYSQL_IT_PASSWORD": password}
                result = subprocess.run([str(ROOT / "mvnw"), "-B", "-ntp", "-Pmysql-it", "clean", "verify"], cwd=ROOT, env=env)
                if result.returncode:
                    raise RuntimeError("MySQL integration verification failed; see Maven test reports above")
                print("Verifying UC login and GAME recovery through the four service processes.", flush=True)
                scenario = [sys.executable, str(ROOT / "scripts/check-realm-march.py"), "--mysql-it-uc"]
                if unity_probe:
                    # 同一隔离区服中为 C# 客户端注册单独账号。使用真实 UC 令牌，避免测试进程接触开发会话。
                    scenario.extend(["--unity-probe", str(unity_probe)])
                subprocess.run(scenario, cwd=ROOT, env=env, check=True, timeout=300)
                print("Verifying two competing marches and a GAME crash while a resource is occupied.", flush=True)
                # Python 消息类由上面的 clean verify 生成，干净检出时在构建成功后才载入。
                from check_realm_mine import verify_mine_contention
                verify_mine_contention(jdbc + PROJECTION_SCHEMA + options, PROJECTION_USER, projection_password,
                                       lambda sql: mysql(identity, db_env, sql, PROJECTION_SCHEMA))

            def stop_mysql():
                change_mysql_state(name, identity, "stop")
                print("Stopped this test run's MySQL; its data volume and port are reserved for the restart.", flush=True)

            def start_mysql():
                change_mysql_state(name, identity, "start")
                wait_mysql(identity, db_env)

            print("Verifying live GAME commands and SQL catch-up across a real MySQL outage.", flush=True)
            from check_realm_projection import verify_projection_recovery
            verify_projection_recovery(jdbc + PROJECTION_SCHEMA + options, PROJECTION_USER, projection_password,
                                       stop_mysql, start_mysql,
                                       lambda sql: mysql(identity, db_env, sql, PROJECTION_SCHEMA))
            print("PASS isolated MySQL verification.", flush=True)
        finally:
            # 清理失败不能掩盖原故障；如果验收本身成功，回收未获确认也必须显式失败。
            original_failure = sys.exc_info()[0] is not None
            if attempted:
                try:
                    if cleanup(name, identity):
                        print("Removed the test container and its anonymous database volume.", flush=True)
                except (OSError, ValueError, RuntimeError, subprocess.SubprocessError):
                    print("Could not confirm cleanup of " + name + "; inspect this test container in Docker.", file=sys.stderr)
                    if not original_failure:
                        raise RuntimeError("Test container cleanup failed") from None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="mysql:8.4", help="official MySQL 8.4 tag or locally cached digest")
    parser.add_argument("--projection-only", action="store_true",
                        help="run the MySQL outage scenario using existing GAME and Gateway jars")
    parser.add_argument("--unity-probe", type=Path, help="exercise a built UnityMarchSmoke.dll with a real UC login")
    args = parser.parse_args()
    if args.unity_probe and (args.projection_only or not args.unity_probe.is_file()):
        parser.error("--unity-probe requires a built probe DLL and the full four-service verification")
    try:
        run(args.image, args.projection_only, args.unity_probe.resolve() if args.unity_probe else None)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        # 不回显可能含容器配置及凭证的底层命令。
        if isinstance(error, subprocess.CalledProcessError):
            print("FAIL A verification subprocess failed; check its preceding output and Docker availability.", file=sys.stderr)
        else:
            print("FAIL " + str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
