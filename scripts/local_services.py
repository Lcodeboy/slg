#!/usr/bin/env python3
"""Start only this checkout's local services; never remove persistent game data."""
# 本地 UC、Battle、GAME 和 Gateway 服务的生命周期入口：配置与密钥持久保存，停止仅针对能够确认归属的进程。
# 原模块字符串也用于命令行帮助，中文维护说明使用注释，保持既有帮助输出不变。
import argparse
import fcntl
import json
import ipaddress
import os
from pathlib import Path
import secrets
import shlex
import signal
import socket
import subprocess
import sys
import time
import uuid
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
RUN = ROOT / "data" / "run"
LOGS = ROOT / "logs"
SERVICES = (("uc", "SLG_UC_PORT", 8081), ("battle", "SLG_BATTLE_RPC_PORT", 9083),
            ("game", "SLG_GAME_PORT", 8082),
            ("gate", "SLG_GATE_PORT", 8080))
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


# 按字面量解析 KEY=value，支持引号和注释；不 source 文件，也不执行变量或命令替换。
def read_env(path):
    """Parse literal KEY=value entries, never execute a shell config file."""
    values = {}
    if not path.exists():
        return values
    for number, raw in enumerate(path.read_text().splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        key, separator, value = line.partition("=")
        if not separator or not key.strip().replace("_", "a").isalnum():
            raise ValueError("%s:%s: expected KEY=value" % (path, number))
        parts = shlex.split(value, comments=True)
        if len(parts) > 1:
            raise ValueError("%s:%s: quote values containing spaces" % (path, number))
        values[key.strip()] = parts[0] if parts else ""
    return values


# 默认与现有开发 MySQL 共用配置；显式文件完整替换默认文件，不混入另一数据库的凭证。
# 只有启动阶段允许补建签名密钥，数据库密码始终由 mysql-local.sh 或调用方提供。
def load_env(env_file=None, create_secret=False):
    env = dict(os.environ)
    persisted = ROOT / "data" / "local.env"
    local_values = read_env(persisted)
    selected = Path(env_file) if env_file is not None else ROOT / "data" / "mysql.env"
    if env_file is not None and not selected.is_file():
        raise ValueError("Configuration file does not exist: " + str(selected))
    configured = read_env(selected)
    # 优先级从低到高：已保存的本地配置、指定配置文件、当前进程环境变量。
    env = {**local_values, **configured, **env}
    validate_database_environment(env)
    if not env.get("SLG_TOKEN_SECRET") and create_secret:
        ROOT.joinpath("data").mkdir(exist_ok=True)
        secret = secrets.token_urlsafe(48)
        if persisted.exists():
            raise ValueError("data/local.env exists but has no SLG_TOKEN_SECRET; fix it without replacing existing data")
        # 排他创建防止并发覆盖，0600 避免随机签名密钥被其他本机用户读取。
        descriptor = os.open(persisted, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as stream:
            stream.write("SLG_TOKEN_SECRET=" + secret + "\n")
        env["SLG_TOKEN_SECRET"] = secret
    if create_secret and len(env.get("SLG_TOKEN_SECRET", "").encode()) < 32:
        raise ValueError("SLG_TOKEN_SECRET must contain at least 32 UTF-8 bytes")
    env.setdefault("SLG_BIND", "127.0.0.1")
    env.setdefault("SLG_GATE_TCP_HOST", env["SLG_BIND"])
    endpoints = (*SERVICES, ("uc-rpc", "SLG_UC_RPC_PORT", 9081), ("game-rpc", "SLG_GAME_RPC_PORT", 9082),
                 ("gameplay-tcp", "SLG_GATE_TCP_PORT", 8090))
    for name, key, default in endpoints:
        env.setdefault(key, str(default))
        port = int(env[key])
        if not 1 <= port <= 65535:
            raise ValueError(key + " is outside 1..65535")
    if len({int(env[key]) for _, key, _ in endpoints}) != len(endpoints):
        raise ValueError("HTTP, gameplay TCP and internal RPC ports must be distinct")
    rpc_host = probe_host(env.get("SLG_RPC_BIND", env["SLG_BIND"]))
    env.setdefault("SLG_UC_RPC_TARGET", endpoint(rpc_host, env["SLG_UC_RPC_PORT"]))
    env.setdefault("SLG_GAME_RPC_TARGET", endpoint(rpc_host, env["SLG_GAME_RPC_PORT"]))
    battle_host = probe_host(env.get("SLG_BATTLE_RPC_BIND", "127.0.0.1"))
    env.setdefault("SLG_BATTLE_RPC_TARGET", endpoint(battle_host, env["SLG_BATTLE_RPC_PORT"]))
    return env


# 启动前只校验配置，不探测投影库是否在线。断开投影库时 GAME 仍应能从原卷接收新命令。
def validate_database_environment(env):
    jdbc = env.get("SLG_UC_JDBC_URL", "jdbc:mysql://127.0.0.1:13306/slg_uc")
    if not jdbc.startswith("jdbc:mysql://"):
        raise ValueError("SLG_UC_JDBC_URL must use MySQL JDBC")
    if not env.get("SLG_UC_DB_PASSWORD", env.get("SLG_DB_PASSWORD")):
        raise ValueError("MySQL requires SLG_UC_DB_PASSWORD or SLG_DB_PASSWORD. "
                         "Run ./scripts/mysql-local.sh start or provide an explicit --env-file.")
    enabled = env.get("SLG_PROJECTION_ENABLED", "true").lower()
    if enabled not in ("true", "false"):
        raise ValueError("SLG_PROJECTION_ENABLED must be true or false")
    if enabled == "true":
        if not env.get("SLG_PROJECTION_JDBC_URL", "").startswith("jdbc:mysql://"):
            raise ValueError("SLG_PROJECTION_JDBC_URL must use MySQL JDBC. Run ./scripts/mysql-local.sh prepare.")
        if not env.get("SLG_PROJECTION_DB_USER", "slg_projection").strip() or not env.get("SLG_PROJECTION_DB_PASSWORD"):
            raise ValueError("SLG_PROJECTION_DB_USER and SLG_PROJECTION_DB_PASSWORD are required. "
                             "For an existing development volume, run ./scripts/mysql-local.sh provision.")


def realm_identity(env):
    """启动要求绑定已存在的目录、区号和 UUID。完整身份由 RocksDB 在开放监听前验证。"""
    directory, realm, volume = (env.get(key, "").strip() for key in
                                ("SLG_REALM_DIR", "SLG_REALM_ID", "SLG_REALM_VOLUME_ID"))
    if not directory or not realm or not volume:
        raise ValueError("Bind SLG_REALM_DIR, SLG_REALM_ID and SLG_REALM_VOLUME_ID to an initialized volume")
    path = Path(directory)
    if not path.is_absolute() or path.is_symlink() or not path.is_dir():
        raise ValueError("SLG_REALM_DIR must be an existing absolute volume directory")
    if not realm.isascii() or not realm.isdecimal() or not 1 <= int(realm) <= 2147483647:
        raise ValueError("SLG_REALM_ID must be a positive Java integer")
    if str(uuid.UUID(volume)) != volume.lower():
        raise ValueError("SLG_REALM_VOLUME_ID must be a canonical UUID")
    return [str(path), str(int(realm)), volume.lower()]


def service_environment(name, env):
    """UC 只取账号库凭证，GAME 只取投影库凭证；只向三个验签进程发放会话密钥。"""
    # 账号配置文件可能同时包含容器管理变量。按服务选出所需配置，未声明的数据库地址与凭证不下发。
    prefixes = {
        "uc": ("SLG_UC_", "SLG_DB_"),
        "battle": ("SLG_BATTLE_RPC_BIND", "SLG_BATTLE_RPC_PORT", "SLG_BATTLE_WORKERS",
                   "SLG_BATTLE_QUEUE_CAPACITY", "SLG_BATTLE_GAME_CLIENT_CA"),
        "game": ("SLG_GAME_WORKERS", "SLG_GAME_QUEUE_", "SLG_GAME_ACCOUNT_", "SLG_GAME_DEADLINE_",
                 "SLG_GAME_PORT", "SLG_GAME_RPC_PORT", "SLG_REALM_", "SLG_BATTLE_RPC_", "SLG_PROJECTION_"),
        "gate": ("SLG_GATE_", "SLG_UC_RPC_TARGET", "SLG_GAME_RPC_TARGET"),
    }
    common = {"SLG_BIND"}
    if name != "battle":
        common.add("SLG_TOKEN_SECRET")
    child = {k: v for k, v in env.items() if not k.startswith("SLG_") or k in common
             or k.startswith(("SLG_RPC_", *prefixes[name]))}
    # 网关向 UC/GAME 发起 RPC；GAME 向 Worker 发起 RPC。双方独立签发的客户端身份分别下发。
    for field in ("CERT", "KEY"):
        generic = "SLG_RPC_CLIENT_TLS_" + field
        if name != "gate":
            child.pop(generic, None)
        if name == "game" and "SLG_BATTLE_CLIENT_TLS_" + field in env:
            child[generic] = env["SLG_BATTLE_CLIENT_TLS_" + field]
        if name == "gate":
            child.pop("SLG_RPC_TLS_" + field, None)
    return child


# 进程记录按服务分开存储，重启后仍可定位之前由启动器创建的进程。
def record_path(name):
    return RUN / (name + ".json")


# 记录损坏时明确失败，不根据不完整的 PID 信息猜测应停止哪个进程。
def read_record(name):
    path = record_path(name)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        raise ValueError("Invalid process record: " + str(path))


# 同时读取启动时间与完整命令行，避免仅凭可能复用的 PID 判断进程归属。
def fingerprint(pid):
    result = subprocess.run(["ps", "-p", str(pid), "-o", "lstart=", "-o", "command="],
                            text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    return result.stdout.strip() if result.returncode == 0 else ""


def canonical_fingerprint(value):
    # macOS 的 Java 启动器可能把 argv[0] 从 java 改成 /usr/bin/java。
    # 只规范化该可执行文件名，启动时间和所有参数仍必须完全一致。
    parts = value.split(None, 5)
    if len(parts) != 6:
        return value
    executable, separator, arguments = parts[5].partition(" ")
    if Path(executable).name == "java":
        executable = "java"
    return tuple(parts[:5]) + (executable, arguments)


# 只有 PID、启动指纹和 jar 路径同时匹配，才允许将进程视为本次工程的服务。
def is_owned(record):
    if not record or not isinstance(record.get("pid"), int) or record["pid"] < 2:
        return False
    current = fingerprint(record["pid"])
    return bool(current and canonical_fingerprint(current) == canonical_fingerprint(record.get("fingerprint", "")) and
                record.get("jar") and ("-jar " + record["jar"]) in current)


def probe_host(bind):
    """具名绑定按原地址拨号，通配监听通过同一 IP 协议族的回环地址探测。"""
    host = bind.removeprefix("[").removesuffix("]")
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return host
    if address.is_unspecified:
        return "127.0.0.1" if address.version == 4 else "::1"
    return host


def endpoint(host, port):
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


def health(port, host="127.0.0.1"):
    try:
        with HTTP.open(f"http://{endpoint(host, port)}/health", timeout=1) as response:
            return response.status == 200
    except (OSError, urllib.error.URLError):
        return False


def service_ready(name, record):
    host, port = record["probe_host"], record["port"]
    if name != "battle":
        if not health(port, host):
            return False
        if name != "gate":
            return True
        # 账号入口健康不能证明 Unity 可以连接。旧进程记录缺少游戏端口，不能按新版复用。
        host, port = record.get("tcp_probe_host"), record.get("tcp_port")
        if not host or not port:
            return False
    # 只探测连接，不占用账号或发放会话。TLS 和鉴权由协议验收单独覆盖。
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


# 选择与绑定地址匹配的协议族。预检用于尽早报告冲突，最终占用以服务 bind 的结果为准。
def available(host, port):
    host = host.removeprefix("[").removesuffix("]")
    for family, kind, proto, _, address in socket.getaddrinfo(host, port, type=socket.SOCK_STREAM):
        with socket.socket(family, kind, proto) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                listener.bind(address)
            except OSError:
                return False
    return True


# 先核验身份再发终止信号；等待期间反复核验，必要时也只强制结束同一个已确认进程。
def stop_one(name, record):
    if not record:
        return
    if not is_owned(record):
        # PID 已复用或命令已变化时不得误杀；只有进程确实不存在才删除旧记录。
        if fingerprint(record.get("pid", 0)):
            raise RuntimeError("%s PID no longer matches this launch record; left untouched" % name)
        record_path(name).unlink(missing_ok=True)
        return
    os.kill(record["pid"], signal.SIGTERM)
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline and is_owned(record):
        time.sleep(0.2)
    if is_owned(record):
        os.kill(record["pid"], signal.SIGKILL)
    record_path(name).unlink(missing_ok=True)
    print("Stopped " + name, flush=True)


# 先完成所有服务的 jar/端口预检，再按 UC、Battle、GAME、Gateway 顺序启动并等待健康。
def start(env, no_build):
    identity = realm_identity(env)
    if not no_build:
        print("Building Java services…", flush=True)
        subprocess.run([str(ROOT / "mvnw"), "-q", "-DskipTests", "package"],
                       cwd=ROOT, env=env, check=True)
    plans = []
    tcp_bind = env["SLG_GATE_TCP_HOST"]
    tcp_listener = {"tcp_probe_host": probe_host(tcp_bind), "tcp_port": int(env["SLG_GATE_TCP_PORT"])}
    for name, key, _ in SERVICES:
        jar = ROOT / ("slg_" + name) / "target" / ("slg_%s-1.0-SNAPSHOT.jar" % name)
        if not jar.is_file():
            raise RuntimeError("Missing executable jar: %s; run ./mvnw package first" % jar)
        command = ["java", "-jar", str(jar)]
        if name == "game":
            command += ["realm", "serve", *identity]
        bind = env.get("SLG_BATTLE_RPC_BIND", "127.0.0.1") if name == "battle" else env["SLG_BIND"]
        host = probe_host(bind)
        old = read_record(name)
        if is_owned(old):
            expected = {"command": command, "port": int(env[key]), "probe_host": host}
            if name == "gate":
                expected.update(tcp_listener)
            if any(old.get(k) != value for k, value in expected.items()):
                raise RuntimeError(f"{name} is running with a different launch record. Stop the local stack first.")
            if not service_ready(name, old):
                raise RuntimeError(f"{name} is running but its configured endpoint is unavailable")
            print("Already running %s (PID %s)" % (name, old["pid"]), flush=True)
            continue
        if old and fingerprint(old.get("pid", 0)):
            raise RuntimeError("%s has a live but mismatched PID record; inspect it before starting" % name)
        if not available(bind, int(env[key])):
            raise RuntimeError("Port %s is already in use; no new services were started" % env[key])
        if name == "gate" and not available(tcp_bind, tcp_listener["tcp_port"]):
            raise RuntimeError("Gameplay TCP port %s is already in use; no new services were started" % tcp_listener["tcp_port"])
        if name in ("uc", "game"):
            rpc_port = int(env["SLG_" + name.upper() + "_RPC_PORT"])
            if not available(env.get("SLG_RPC_BIND", env["SLG_BIND"]), rpc_port):
                raise RuntimeError("RPC port %s is already in use; no new services were started" % rpc_port)
        plans.append((name, key, jar, command, host))
    LOGS.mkdir(exist_ok=True)
    launched = []
    try:
        for name, key, jar, command, host in plans:
            logfile = LOGS / (name + ".log")
            with logfile.open("ab") as output:
                # MySQL root 初始化密码只属于数据库容器，不传给 Java 应用进程。
                service_env = service_environment(name, env)
                child = subprocess.Popen(command, cwd=ROOT, env=service_env,
                                         stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT,
                                         start_new_session=True)
            # 启动后立即记录身份；后续失败回滚只处理本轮新建进程，不动此前已有服务。
            record = {"pid": child.pid, "jar": str(jar), "port": int(env[key]), "probe_host": host, "command": command,
                      "fingerprint": fingerprint(child.pid)}
            if name == "gate":
                record.update(tcp_listener)
            if not record["fingerprint"]:
                raise RuntimeError("%s exited before startup; inspect %s" % (name, logfile))
            record_path(name).write_text(json.dumps(record, indent=2) + "\n")
            launched.append((name, record))
            deadline = time.monotonic() + 45
            while time.monotonic() < deadline:
                if child.poll() is not None:
                    raise RuntimeError("%s exited with %s; inspect %s" % (name, child.returncode, logfile))
                if service_ready(name, record):
                    print(f"Ready {name} at {endpoint(host, env[key])}", flush=True)
                    break
                time.sleep(0.2)
            else:
                raise RuntimeError("%s did not become healthy; inspect %s" % (name, logfile))
        print("Gateway: http://" + endpoint(probe_host(env["SLG_BIND"]), env["SLG_GATE_PORT"]), flush=True)
        print("Unity gameplay: " + endpoint(tcp_listener["tcp_probe_host"], tcp_listener["tcp_port"]), flush=True)
        print("Demo: python3 scripts/demo.py", flush=True)
        print("Stop: ./scripts/stop-local.sh (database files are preserved)", flush=True)
    except BaseException:
        # 按相反顺序回收本轮启动结果，保留数据库、日志和密钥用于继续运行或排查。
        for name, record in reversed(launched):
            try:
                stop_one(name, record)
            except (OSError, RuntimeError) as cleanup_error:
                print("Cleanup: " + str(cleanup_error), file=sys.stderr)
        raise


# 统一解析启动/停止/状态命令；文件锁使同一工作目录的多个启动器不能并发改进程记录。
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "stop", "status"))
    parser.add_argument("--no-build", action="store_true", help="start existing packaged jars")
    parser.add_argument("--env-file", type=Path,
                        help="configuration file; defaults to data/mysql.env (explicit file replaces it)")
    args = parser.parse_args()
    RUN.mkdir(parents=True, exist_ok=True)
    with (RUN / "launcher.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if args.action == "start":
            start(load_env(args.env_file, create_secret=True), args.no_build)
        elif args.action == "stop":
            for name, _, _ in reversed(SERVICES):
                stop_one(name, read_record(name))
            print("Persistent data retained in data/", flush=True)
        else:
            for name, _, _ in SERVICES:
                record = read_record(name)
                owned = is_owned(record)
                if owned and "probe_host" not in record:
                    status = "running, restart required for current launch configuration"
                elif owned:
                    status = "healthy" if service_ready(name, record) else "running, not healthy"
                else:
                    status = "stopped"
                print("%s: %s" % (name, status))


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        print("Error: " + str(error), file=sys.stderr)
        sys.exit(1)
