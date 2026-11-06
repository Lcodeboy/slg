"""Public gateway client and session helpers for local realm verification."""
import base64
import hashlib
import hmac
import json
import os
import secrets
import socket
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from local_services import health
from tcp_client import DEFAULT_TCP_PORT, TcpClient, player_pb


# 账号和探活走 HTTP，玩法命令走同一个账号的 TCP 会话；凭证只保留在内存。
class Api:
    # 每个实例代表一个账号会话；禁用系统代理，保证本地请求直达配置中的网关。
    def __init__(self, base_url, tcp_port=None, tcp_host=None, tls=None):
        self.base_url = base_url.rstrip("/")
        self.tcp_host = tcp_host or os.getenv("SLG_GATE_TCP_HOST") or urllib.parse.urlsplit(base_url).hostname
        self.tcp_port = tcp_port if tcp_port is not None else int(os.getenv("SLG_GATE_TCP_PORT", DEFAULT_TCP_PORT))
        self.tls = tls
        self._tcp = self._tcp_token = None
        self._last_use = 0
        self.token = None
        self.account_id = None
        self.http = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    # 同时检查 HTTP 状态和业务 success；只有预期验证拒绝场景时才返回错误给调用方断言。
    def request(self, path, body=None, allow_error=False):
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(self.base_url + path, data=data, headers=headers)
        try:
            with self.http.open(request, timeout=15) as response:
                result = json.load(response)
        except urllib.error.HTTPError as error:
            try:
                result = json.loads(error.read())
            except ValueError:
                raise RuntimeError("%s returned HTTP %s" % (path, error.code)) from None
            if not allow_error:
                raise RuntimeError("%s failed: %s" % (path, result.get("error", error.code))) from None
        if result.get("success") is False and not allow_error:
            raise RuntimeError("%s failed: %s" % (path, result.get("error")))
        return result

    # 新操作生成新请求 ID；调用方显式传入原 ID 时用于模拟丢失响应后的幂等重试。
    def play(self, message, request_id=None, allow_error=False):
        if not self.token:
            raise ValueError("A gameplay session requires a login token")
        # 同步验收没有后台心跳协程；长时间停在故障注入步骤后，发送前先重新鉴权。
        # 已经写入 socket 后遇到异常则交给调用方按原 ID 查回执，绝不自动重发命令。
        if self._tcp and (self.token != self._tcp_token or time.monotonic() - self._last_use > 15):
            self.close()
        if self._tcp is None:
            self._tcp = TcpClient(self.tcp_host, self.tcp_port, self.token, self.tls)
            self._tcp_token = self.token
        try:
            result = self._tcp.play(request_id or str(uuid.uuid4()), message)
            self._last_use = time.monotonic()
        except BaseException:
            self.close()
            raise
        if result.error is not None and not allow_error:
            raise RuntimeError(f"{message.DESCRIPTOR.name} failed: {result.error.code}")
        return result

    def close(self):
        if self._tcp is not None:
            self._tcp.close()
            self._tcp = self._tcp_token = None

    def wait_for_empty_player(self):
        """用无副作用读取确认 Gateway 已能访问 GAME；探活成功不等于业务通道已完成握手。"""
        wait_for("Gateway reaches GAME before the first mutation",
                 lambda: self.play(player_pb.GetPlayerReq(), allow_error=True),
                 lambda r: r.error is not None and r.error.code == "PLAYER_NOT_FOUND", 20)

    # 随机账号隔离每轮演示，并验证注册与登录解析到同一账号身份。
    def create_account(self, suffix):
        username = "demo_" + suffix + "_" + secrets.token_hex(5)
        password = "Demo-" + secrets.token_urlsafe(20)
        registered = self.request("/api/auth/register", {"username": username, "password": password})["data"]
        logged_in = self.request("/api/auth/login", {"username": username, "password": password})["data"]
        require(logged_in["accountId"] == registered["accountId"], "Login changed account identity")
        self.account_id = logged_in["accountId"]
        require(isinstance(self.account_id, str) and self.account_id.isascii() and self.account_id.isdigit()
                and str(int(self.account_id)) == self.account_id and 0 < int(self.account_id) <= 2**63 - 1,
                "UC did not return a canonical positive BIGINT string")
        self.token = logged_in["token"]
        return username


# 将业务断言转成统一失败路径，避免依赖可被 Python 优化选项关闭的 assert。
def require(condition, message):
    if not condition:
        raise RuntimeError(message)


# 按条件轮询真实异步任务；单调时钟控制超时，避免本机时间校准影响等待边界。
def wait_for(label, probe, predicate, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = probe()
        if predicate(value):
            print("PASS " + label, flush=True)
            return value
        time.sleep(0.5)
    raise RuntimeError("Timed out: " + label)


def session_token(account, secret):
    """仅为本次隔离 fixture 签发 slg1 会话；不读取或使用开发环境签名密钥。"""
    encode = lambda value: base64.urlsafe_b64encode(value).rstrip(b"=").decode()
    now = int(time.time())
    payload = {"accountId": account, "issuedAt": now, "expiresAt": now + 3600}
    signed = "slg1." + encode(json.dumps(payload, separators=(",", ":")).encode())
    return signed + "." + encode(hmac.new(secret.encode(), signed.encode(), hashlib.sha256).digest())


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(10)


def rpc_listening(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            return True
    except OSError:
        return False


def launch(stack, name, java, env, port, directory, probe=health):
    log = directory / (name + ".log")
    with log.open("wb") as output:
        process = subprocess.Popen(java, env=env, stdout=output, stderr=subprocess.STDOUT)
    stack.callback(stop, process)
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline and process.poll() is None:
        if probe(port):
            return process
        time.sleep(0.1)
    raise RuntimeError(f"{name} failed to start: {log.read_text(errors='replace')[-2000:]}")
