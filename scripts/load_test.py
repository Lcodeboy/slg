"""Bounded open-arrival measurement with disposable infrastructure and explicit outcomes."""
# 压测进程独立控制到达计划；响应变慢不会悄悄降低目标速率，积压必须体现为排队或丢弃。
import argparse
import asyncio
from array import array
from collections import Counter
from dataclasses import dataclass, field
import json
import math
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
from tempfile import TemporaryDirectory
from contextlib import ExitStack
import uuid
import time

from realm_fixture import launch, rpc_listening, session_token
from tcp_client import AsyncTcpClient, infantry_order, march_pb, player_pb

ROOT = Path(__file__).resolve().parent.parent
WRITES = {march_pb.StartMarchReq.DESCRIPTOR.name}


def report_path(path):
    """在启动任何进程前拒绝不安全输出位置，报告也不能覆盖任何既有文件。"""
    path = Path(path).resolve()
    if path == ROOT or ROOT / "data" in path.parents or path.name.startswith(".env"):
        raise ValueError("Report output must not target runtime data or credential files")
    if path.exists():
        raise ValueError("Report output already exists; choose a new filename")
    return path


def private_json(path, value):
    """报告不可覆盖已有文件，也不能写入默认运行数据、凭证或进程记录。"""
    path = report_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.write("\n")


def percentiles(values):
    if not values:
        return {"count": 0, "p50": None, "p95": None, "p99": None, "max": None}
    ordered = sorted(values)
    pick = lambda fraction: round(ordered[max(0, math.ceil(len(ordered) * fraction) - 1)], 3)
    return {"count": len(values), "p50": pick(.5), "p95": pick(.95), "p99": pick(.99), "max": round(ordered[-1], 3)}


@dataclass
class Attempt:
    # write() 已交给传输层就可能到达服务端，随后异常必须归为结果未知。
    sent: bool = False


@dataclass
class Stats:
    offered: int = 0
    sent: int = 0
    success: int = 0
    reject: int = 0
    unknown: int = 0
    not_sent: int = 0
    write_success: int = 0
    steady_success: int = 0
    steady_write_success: int = 0
    business_rejected: int = 0
    codes: Counter = field(default_factory=Counter)
    commands: Counter = field(default_factory=Counter)
    latency: array = field(default_factory=lambda: array("d"))
    success_latency: array = field(default_factory=lambda: array("d"))
    queue_wait: array = field(default_factory=lambda: array("d"))
    max_queue: int = 0

    def outcome(self, attempt, command, outcome, code, elapsed_ms, queue_ms):
        self.codes[code] += 1
        if not attempt.sent:
            self.not_sent += 1
            return
        self.sent += 1
        self.commands[command] += 1
        self.latency.append(elapsed_ms)
        self.queue_wait.append(queue_ms)
        if outcome == "success":
            self.success += 1
            self.success_latency.append(elapsed_ms)
            self.write_success += command in WRITES
        elif outcome == "reject":
            self.reject += 1
            self.business_rejected += code not in {"SERVER_BUSY", "UPSTREAM_BUSY", "DEADLINE_EXCEEDED", "RATE_LIMITED", "BUSY"} and not code.startswith("STATUS_")
        else:
            self.unknown += 1

    def report(self, duration, wall_seconds):
        return {"offered": self.offered, "sent": self.sent, "success": self.success,
                "reject": self.reject, "unknown": self.unknown, "not_sent": self.not_sent,
                "write_success": self.write_success,
                "business_rejected": self.business_rejected,
                "steady_window_success_per_second": self.steady_success / duration,
                "steady_window_write_success_per_second": self.steady_write_success / duration,
                "success_per_scheduled_second": self.success / duration,
                "write_success_per_scheduled_second": self.write_success / duration,
                "success_per_wall_second": self.success / wall_seconds,
                "wall_seconds_including_drain": wall_seconds,
                "latency_from_scheduled_ms": percentiles(self.latency),
                "success_latency_from_scheduled_ms": percentiles(self.success_latency),
                "client_queue_wait_ms": percentiles(self.queue_wait),
                "max_client_queue": self.max_queue, "codes": dict(self.codes),
                "commands_sent": dict(self.commands),
                "accounting_valid": self.offered == self.sent + self.not_sent
                                    and self.sent == self.success + self.reject + self.unknown}


def classify(response, request_id):
    """按收到的业务错误区分确定拒绝与结果未知。前者才能计入拒绝率。"""
    if response.request_id != request_id:
        return "unknown", "INVALID_RESPONSE"
    if response.error is None:
        return ("success", "OK") if response.status == 200 and response.body is not None else ("unknown", "INVALID_RESPONSE")
    code = response.error.code
    if code in {"RESULT_UNKNOWN", "STORAGE_UNAVAILABLE", "UPSTREAM_TIMEOUT_UNKNOWN", "UPSTREAM_UNAVAILABLE", "UPSTREAM_UNAVAILABLE_UNKNOWN"}:
        return "unknown", code
    if response.status >= 500 and code not in {"SERVER_BUSY", "UPSTREAM_BUSY", "DEADLINE_EXCEEDED", "RATE_LIMITED", "BUSY"}:
        return "unknown", code
    return "reject", code


def workload(sequence, allow_writes):
    if allow_writes and sequence % 5 == 0:
        order = infantry_order(150, 150, 1, march_pb.MARCH_MISSION_GATHER)
        order.heroes.SetInParent()  # 多列行军独立压测兵力预留，明确不占默认英雄。
        return order
    return player_pb.GetPlayerReq()


async def run_phase(clients, rate, duration, queue_size, timeout, drain, allow_writes, run_id):
    """任务数由连接数固定，上游再慢也不会创建每请求一个的无界协程。"""
    queue, stats = asyncio.Queue(maxsize=queue_size), Stats()
    loop = asyncio.get_running_loop()
    start = loop.time()
    total = int(rate * duration)
    per_account = Counter()
    exercised = set()

    async def worker(client):
        while True:
            index, scheduled = await queue.get()
            attempt, command = Attempt(), ""
            began = loop.time()
            try:
                account_index = client.account_index
                exercised.add(account_index)
                ordinal = per_account[account_index]
                per_account[account_index] += 1
                message = workload(account_index + ordinal, allow_writes)
                command = message.DESCRIPTOR.name
                if began - scheduled >= timeout:
                    stats.outcome(attempt, command, "unknown", "CLIENT_QUEUE_EXPIRED", 0, 0)
                    continue
                request_id = "%s-%s" % (run_id, index)
                response = await asyncio.wait_for(client.play(request_id, message, attempt),
                                                          timeout - (began - scheduled))
                outcome, code = classify(response, request_id)
                stats.outcome(attempt, command, outcome, code, (loop.time()-scheduled)*1000, (began-scheduled)*1000)
                if outcome == "success" and loop.time() <= start + duration:
                    stats.steady_success += 1
                    stats.steady_write_success += command in WRITES
            except asyncio.CancelledError:
                stats.outcome(attempt, command, "unknown", "DRAIN_CANCELLED", (loop.time()-scheduled)*1000, (began-scheduled)*1000)
                raise
            except Exception as error:
                code = "TIMEOUT" if isinstance(error, asyncio.TimeoutError) else "TRANSPORT_ERROR"
                stats.outcome(attempt, command, "unknown", code, (loop.time()-scheduled)*1000, (began-scheduled)*1000)
                await client.close()
            finally:
                queue.task_done()

    workers = [asyncio.create_task(worker(client)) for client in clients] if total else []
    try:
        for index in range(total):
            scheduled = start + index / rate
            delay = scheduled - loop.time()
            if delay > 0:
                await asyncio.sleep(delay)
            stats.offered += 1
            try:
                queue.put_nowait((index, scheduled))
                stats.max_queue = max(stats.max_queue, queue.qsize())
            except asyncio.QueueFull:
                stats.not_sent += 1
                stats.codes["CLIENT_QUEUE_FULL"] += 1
            # 迟到时仍按计划计数，但定期让出事件循环，避免生产器饿死客户端收包协程。
            if index % 128 == 0:
                await asyncio.sleep(0)
        await asyncio.sleep(max(0, start + duration - loop.time()))
        try:
            await asyncio.wait_for(queue.join(), drain)
        except asyncio.TimeoutError:
            pass
    finally:
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        while not queue.empty():
            queue.get_nowait(); queue.task_done()
            stats.not_sent += 1; stats.codes["DRAIN_UNSENT"] += 1
    result = stats.report(duration, max(.001, loop.time() - start))
    result["rate"] = rate
    result["duration"] = duration
    result["unique_accounts_exercised"] = len(exercised)
    result["all_scheduled_requests_eventually_succeeded"] = stats.not_sent == stats.reject == stats.unknown == 0
    p99 = result["success_latency_from_scheduled_ms"]["p99"]
    result["target_phase_pass"] = (result["steady_window_success_per_second"] >= .99 * rate
        and p99 is not None and p99 <= 500 and stats.not_sent == stats.reject == stats.unknown == 0) if rate else None
    return result


async def seed_players(port, accounts, clients):
    # 复用已鉴权的长连建角，万连接场景不重复占用第二组临时端口。
    # 若测试专门预建更多离线角色，仅这些账号使用短连接；限并发以隔离准备成本。
    failures = Counter()
    pending = iter(enumerate(accounts))

    async def seed():
        for index, account in pending:
            offline = index >= len(clients)
            client = AsyncTcpClient("127.0.0.1", port, index, account["token"]) if offline else clients[index]
            request_id = "seed-" + str(index)
            try:
                if offline:
                    await asyncio.wait_for(client.connect(), 15)
                response = await asyncio.wait_for(
                    client.play(request_id, player_pb.CreatePlayerReq(name="load-" + str(index))), 15)
                outcome, code = classify(response, request_id)
                if outcome != "success":
                    failures[code] += 1
            except Exception:
                failures["SEED_TRANSPORT_ERROR"] += 1
            finally:
                if offline:
                    await client.close()

    await asyncio.gather(*(seed() for _ in range(min(16, len(accounts)))))
    if failures:
        raise RuntimeError("Player fixture preparation failed: " + json.dumps(dict(failures)))


async def measure(args, port, accounts):
    clients = [AsyncTcpClient("127.0.0.1", port, i % len(accounts), accounts[i % len(accounts)]["token"])
               for i in range(args.connections)]
    try:
        for i in range(0, len(clients), 32):
            await asyncio.wait_for(asyncio.gather(*(c.connect() for c in clients[i:i+32])), 20)
        await seed_players(port, accounts, clients)
        result = await run_phase(clients, args.rate, args.duration, args.queue_size, 5, 10,
                                 args.workload == "march-mix", "load-"+secrets.token_hex(4))
        held = sum(c.alive for c in clients)
        result["target_phase_pass"] = result["target_phase_pass"] and held == args.connections
        return {**result, "transport": "tcp", "connections": args.connections, "players": args.players,
                "workload": args.workload, "heap_per_jvm_mib": args.heap_mib, "tcp_held_at_end": held}
    finally:
        for i in range(0, len(clients), 64):
            await asyncio.gather(*(c.close() for c in clients[i:i+64]), return_exceptions=True)


def run(args):
    # 使用专属临时卷、端口、签名密钥及进程。压测不访问开发账号库，不把注册/登录成本计入命令延迟。
    with TemporaryDirectory(prefix="slg-realm-load-") as tmp, ExitStack() as stack:
        root = Path(tmp)
        volume = root / "realm"
        volume.mkdir()
        with ExitStack() as ports:
            sockets = [ports.enter_context(socket.socket()) for _ in range(6)]
            for sock in sockets:
                sock.bind(("127.0.0.1", 0))
            health_port, rpc, gate, tcp_port, worker, unused_uc = (sock.getsockname()[1] for sock in sockets)
        secret = secrets.token_hex(32)
        env = {k: v for k, v in os.environ.items() if not k.startswith("SLG_")}
        # 此场景只测本地提交链路，独立的 MySQL 故障验收显式启用投影。
        env.update(SLG_PROJECTION_ENABLED="false", SLG_TOKEN_SECRET=secret, SLG_BIND="127.0.0.1", SLG_GAME_PORT=str(health_port),
                   SLG_GAME_RPC_PORT=str(rpc), SLG_GAME_RPC_TARGET=f"127.0.0.1:{rpc}", SLG_GATE_PORT=str(gate),
                   SLG_BATTLE_RPC_PORT=str(worker), SLG_BATTLE_RPC_TARGET=f"127.0.0.1:{worker}",
                   SLG_UC_RPC_TARGET=f"127.0.0.1:{unused_uc}", SLG_GATE_IP_REQUESTS_PER_MINUTE="1000000",
                   SLG_GATE_TCP_PORT=str(tcp_port), SLG_GATE_AUTH_PER_MINUTE="1000000",
                   SLG_GATE_COMMANDS_PER_SECOND="10000")
        jars = {name: ROOT / f"slg_{name}/target/slg_{name}-1.0-SNAPSHOT.jar" for name in ("game","battle","gate")}
        if any(not path.is_file() for path in jars.values()):
            raise RuntimeError("Build the application jars before running the load measurement")
        commands = {key: ["java", f"-Xmx{args.heap_mib}m", "-jar", str(jar)] for key, jar in jars.items()}
        identity = [str(volume), "1", str(uuid.uuid4())]
        subprocess.run(commands["game"] + ["volume", "init", *identity], env=env, check=True, timeout=30)
        worker_env = {k: v for k, v in env.items() if k != "SLG_TOKEN_SECRET"}
        launch(stack, "battle", commands["battle"], worker_env, worker, root, rpc_listening)
        launch(stack, "game", commands["game"] + ["realm", "serve", *identity], env, health_port, root)
        launch(stack, "gate", commands["gate"], env, gate, root)
        accounts = [{"token": session_token(str(i + 1), secret)} for i in range(args.players)]
        return asyncio.run(measure(args, tcp_port, accounts))


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--transport", choices=("tcp",), default="tcp")
    parser.add_argument("--workload", choices=("player-read", "march-mix"), default="player-read")
    parser.add_argument("--players", type=int, default=32, help="total characters seeded on the isolated realm")
    parser.add_argument("--connections", type=int, default=32, help="held TCP sessions, each bound to one seeded account")
    parser.add_argument("--rate", type=int, default=100, help="scheduled commands per second")
    parser.add_argument("--duration", type=int, default=5, help="measurement window in seconds")
    parser.add_argument("--queue-size", type=int, default=256)
    parser.add_argument("--heap-mib", type=int, default=160, help="maximum heap for each of three JVMs")
    parser.add_argument("--output", type=Path, help="also write a private JSON report to a new file")
    args = parser.parse_args(argv)
    if not (1 <= args.rate <= 20000 and 1 <= args.duration <= 300 and args.rate*args.duration <= 1000000
            and 1 <= args.connections <= 12000 and 1 <= args.players <= 20000
            and 1 <= args.queue_size <= 65536 and 96 <= args.heap_mib <= 4096):
        parser.error("Measurement size or JVM heap outside supported bounds")
    if args.output:
        report_path(args.output)
    return args


def main(argv=None):
    try:
        args = arguments(argv)
        report = run(args)
        print(json.dumps(report, indent=2))
        if args.output:
            private_json(args.output, report)
        return 0 if report["target_phase_pass"] else 1
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as error:
        print("Measurement failed: " + str(error), file=sys.stderr)
        return 1
