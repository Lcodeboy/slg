#!/usr/bin/env python3
"""在临时 GAME 和 Gateway 上验证磁盘压力只拒绝新客户端写入。"""
from contextlib import ExitStack
import os
from pathlib import Path
import secrets
import socket
import subprocess
from tempfile import TemporaryDirectory
import uuid

from tcp_client import infantry_order, march_pb, player_pb
from realm_fixture import Api, launch, require, session_token, stop, wait_for

ROOT = Path(__file__).resolve().parent.parent


def run():
    # 只初始化这次运行独有的空目录。进程、端口、密钥都自行分配，无需读取开发库或开发区服的配置。
    with TemporaryDirectory(prefix="slg-realm-disk-") as directory, ExitStack() as stack:
        root = Path(directory)
        volume = root / "realm"
        volume.mkdir()
        volume_id = str(uuid.uuid4())
        with ExitStack() as ports:
            sockets = [ports.enter_context(socket.socket()) for _ in range(6)]
            for sock in sockets:
                sock.bind(("127.0.0.1", 0))
            tcp_port, gate_port, health_port, game_rpc, unused_uc, unused_worker = [s.getsockname()[1] for s in sockets]

        secret = secrets.token_hex(32)
        localhost = "127.0.0.1"
        env = {k: v for k, v in os.environ.items() if not k.startswith("SLG_")}
        env.update(SLG_PROJECTION_ENABLED="false", SLG_BIND=localhost, SLG_TOKEN_SECRET=secret,
                   SLG_GATE_TCP_PORT=str(tcp_port), SLG_GATE_PORT=str(gate_port), SLG_GAME_PORT=str(health_port), SLG_GAME_RPC_PORT=str(game_rpc),
                   SLG_GAME_RPC_TARGET=f"{localhost}:{game_rpc}", SLG_UC_RPC_TARGET=f"{localhost}:{unused_uc}",
                   SLG_BATTLE_RPC_TARGET=f"{localhost}:{unused_worker}")
        java = ["java", "-Xmx160m", "-jar"]
        game = java + [str(ROOT / "slg_game/target/slg_game-1.0-SNAPSHOT.jar")]
        gate = java + [str(ROOT / "slg_gate/target/slg_gate-1.0-SNAPSHOT.jar")]
        identity = [str(volume), "1", volume_id]
        subprocess.run(game + ["volume", "init", *identity], cwd=root, env=env, check=True, timeout=20)
        serve = game + ["realm", "serve", *identity]

        # 始终读取真实临时卷的可用字节。只改变这个测试进程的阈值，不填盘、不修改系统配额。
        normal = {**env, "SLG_REALM_DISK_WARN_BYTES": str(2 << 20), "SLG_REALM_DISK_HARD_BYTES": str(1 << 20)}
        hard = (1 << 63) - 2
        under_pressure = {**env, "SLG_REALM_DISK_WARN_BYTES": str(hard + 1), "SLG_REALM_DISK_HARD_BYTES": str(hard)}
        first = launch(stack, "game-normal", serve, normal, health_port, root)
        launch(stack, "gateway", gate, env, gate_port, root)
        health = Api(f"http://{localhost}:{health_port}")
        owner = Api(f"http://{localhost}:{gate_port}", tcp_port, "127.0.0.1")
        stack.callback(owner.close)
        owner.token = session_token("101", secret)
        second_player = Api(owner.base_url, tcp_port, "127.0.0.1")
        stack.callback(second_player.close)
        second_player.token = session_token("102", secret)

        def probe():
            return health.request("/health")

        wait_for("new disk guard reports free space", probe,
                 lambda h: h["disk"]["state"] == "NORMAL" and not h["disk"]["clientWritesLimited"], 15)
        owner.wait_for_empty_player()
        owner.play(player_pb.CreatePlayerReq(name="Alice"), "create-owner")
        # 行程足够长，以便切换阈值时仍有在途部队；不用改时钟或内存里的任务状态。
        departure = infantry_order(650, 650, 120)
        sent = owner.play(departure, "gather")
        march_id = sent.body.march.march_id
        require(sent.body.march.state == march_pb.MARCH_STATE_OUTBOUND, "Expected an outbound march")
        stop(first)

        limited_game = launch(stack, "game-hard-limit", serve, under_pressure, health_port, root)
        wait_for("same realm volume reaches the artificial hard limit", probe,
                 lambda h: h["volumeId"] == volume_id and h["disk"]["state"] == "HARD_LIMIT"
                 and h["disk"]["clientWritesLimited"] and h["disk"]["usableBytes"] <= hard, 15)
        wait_for("player remains readable under disk pressure",
                 lambda: owner.play(player_pb.GetPlayerReq(), allow_error=True), lambda r: r.error is None, 15)
        require(owner.play(departure, "gather") == sent,
                "An already-confirmed march must return its original receipt")
        create = player_pb.CreatePlayerReq(name="Bob")
        limited = second_player.play(create, "create-after-recovery", allow_error=True)
        require(limited.error.code == "DISK_PRESSURE",
                "A new client write must receive a retryable disk limit")

        # 全部后续写入均由原卷持久化的待办触发。先等水位独立推进，再读玩家，排除“读时补做”的假通过。
        wait_for("system tasks finish the already-admitted march under pressure", probe,
                 lambda h: h["journalSeq"] == 5 and h["disk"]["state"] == "HARD_LIMIT", 60)
        player = owner.play(player_pb.GetPlayerReq()).body.player
        march = owner.play(march_pb.GetMarchReq(march_id=march_id)).body.march
        require(march.state == march_pb.MARCH_STATE_COMPLETED and player.troops["INFANTRY"] == 3000
                and player.resources["WOOD"] == 600 and not player.active_march_ids
                and not player.heroes[0].HasField("assigned_march"),
                "Disk pressure lost troops, duplicated cargo, or failed to release a reservation")
        require(probe()["journalSeq"] == 5,
                "Only player creation, departure and the three background decisions may occupy journal sequences")
        stop(limited_game)

        launch(stack, "game-recovered", serve, normal, health_port, root)
        wait_for("admission reopens once disk readings are normal", probe,
                 lambda h: h["volumeId"] == volume_id and h["journalSeq"] == 5
                 and h["disk"]["state"] == "NORMAL" and not h["disk"]["clientWritesLimited"], 15)
        wait_for("Gateway reconnects after recovery", lambda: owner.play(player_pb.GetPlayerReq(), allow_error=True),
                 lambda r: r.error is None, 15)
        # 如果保护时为暂拒写了失败回执，这个原 ID 将永远失败，或者错误地占用第 6 个日志序号。
        created = second_player.play(create, "create-after-recovery")
        require(second_player.play(create, "create-after-recovery") == created
                and probe()["journalSeq"] == 6, "Retry must commit once and return the same receipt thereafter")
        print("PASS only new writes were deferred; 120 troops returned, 600 wood credited once, "
              "the original request ID was reusable", flush=True)


if __name__ == "__main__":
    run()
