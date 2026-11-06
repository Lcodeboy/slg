#!/usr/bin/env python3
"""在独立临时卷上强杀 GAME，验证采集与 NPC 战斗都能跨进程恢复且只交付一次。"""
import argparse
from contextlib import ExitStack
import os
from pathlib import Path
import re
import secrets
import socket
import subprocess
from tempfile import TemporaryDirectory
import time
import uuid

from tcp_client import battle_result_pb, infantry_order, march_pb, player_pb, receipt_pb, report_pb
from realm_fixture import Api, launch, require, rpc_listening, session_token, wait_for


ROOT = Path(__file__).resolve().parent.parent


def authenticate(api, label, secret, with_uc):
    if with_uc:
        api.create_account(label)
    else:
        api.account_id = {"player": "101", "visitor": "102", "unity": "104"}[label]
        api.token = session_token(api.account_id, secret)


def check_unity_march(probe, api, port):
    """C# 在同一个 UC 账号派三次兵，Python 用独立连接按行军 ID 复核原卷的最终状态。"""
    call = subprocess.run(["dotnet", str(probe), str(port)], input=api.token + "\n",
                          stdout=subprocess.PIPE, text=True, check=True, timeout=105)
    ids = call.stdout.strip().splitlines()
    require(len(ids) == 3 and len(set(ids)) == 3, "Expected distinct gathering, recalled, and NPC march IDs")
    for march_id in ids:
        march = api.play(march_pb.GetMarchReq(march_id=march_id)).body.march
        require(march.march_id == march_id and march.state == march_pb.MARCH_STATE_COMPLETED,
                "A C# march did not complete")
    report = api.play(report_pb.GetBattleReq(battle_id=ids[2])).body
    require(report.state == report_pb.REPORT_SETTLED and len(report.result.attacker.groups) == 1,
            "C# attack did not produce a settled battle report")
    group, = report.result.attacker.groups
    final = api.play(player_pb.GetPlayerReq()).body.player
    require(group.initial_count == 650 and final.troops["INFANTRY"] == 2350 + group.survivors
            and final.wounded.get("INFANTRY", 0) == group.wounded and final.dead.get("INFANTRY", 0) == group.dead
            and final.resources == {"WOOD": 1100, "FOOD": 500, "GEMS": 10}
            and not final.active_march_ids and not final.heroes[0].HasField("assigned_march"),
            "C# gathering, recall, and battle do not match the final army and resources")
    print("PASS all seven generated Unity APIs: reconnect/retry, one gather reward, recall and settled NPC report")


def run(with_uc=False, unity_probe=None):
    with TemporaryDirectory(prefix="slg-realm-march-") as tmp, ExitStack() as stack:
        root = Path(tmp)
        volume = root / "realm"
        volume.mkdir()
        # 同时绑定随机端口，防止本次测试端口互相重合。
        with ExitStack() as ports:
            sockets = [ports.enter_context(socket.socket()) for _ in range(7)]
            for sock in sockets:
                sock.bind(("127.0.0.1", 0))
            tcp_port, gate_port, rpc_port, health_port, uc_rpc, battle_port, uc_health = [s.getsockname()[1] for s in sockets]
        secret = secrets.token_hex(32)
        env = {k: v for k, v in os.environ.items() if not k.startswith("SLG_")}
        # 此场景只测本地提交链路，独立的 MySQL 故障验收显式启用投影。
        env.update(SLG_PROJECTION_ENABLED="false", SLG_TOKEN_SECRET=secret, SLG_BIND="127.0.0.1", SLG_REALM_MAX_RESIDENTS="1",
                   SLG_GAME_PORT=str(health_port), SLG_GAME_RPC_PORT=str(rpc_port),
                   SLG_GATE_TCP_PORT=str(tcp_port), SLG_GATE_PORT=str(gate_port), SLG_GAME_RPC_TARGET=f"127.0.0.1:{rpc_port}",
                   SLG_UC_RPC_TARGET=f"127.0.0.1:{uc_rpc}", SLG_BATTLE_RPC_PORT=str(battle_port),
                   SLG_BATTLE_RPC_TARGET=f"127.0.0.1:{battle_port}")
        java = ["java", "-Xmx160m", "-jar"]
        game = java + [str(ROOT / "slg_game/target/slg_game-1.0-SNAPSHOT.jar")]
        gate = java + [str(ROOT / "slg_gate/target/slg_gate-1.0-SNAPSHOT.jar")]
        worker = java + [str(ROOT / "slg_battle/target/slg_battle-1.0-SNAPSHOT.jar")]
        identity = [str(volume), "1", str(uuid.uuid4())]
        subprocess.run(game + ["volume", "init"] + identity, env=env, check=True, timeout=20)
        serve = game + ["realm", "serve"] + identity
        if with_uc:
            # 只接受隔离脚本在回环随机端口创建的测试 schema，凭证仅交给 UC 子进程。
            jdbc = os.environ.get("SLG_MYSQL_IT_UC_URL", "")
            require(re.fullmatch(r"jdbc:mysql://127\.0\.0\.1:[0-9]+/slg_uc_it\?[^\s]+", jdbc),
                    "Expected the disposable MySQL integration URL")
            uc_env = {**env, "SLG_UC_PORT": str(uc_health), "SLG_UC_RPC_PORT": str(uc_rpc),
                      "SLG_UC_JDBC_URL": jdbc, "SLG_UC_DB_USER": os.environ["SLG_MYSQL_IT_USER"],
                      "SLG_UC_DB_PASSWORD": os.environ["SLG_MYSQL_IT_PASSWORD"]}
            uc = java + [str(ROOT / "slg_uc/target/slg_uc-1.0-SNAPSHOT.jar")]
            launch(stack, "uc", uc, uc_env, uc_health, root)
        first = launch(stack, "game", serve, env, health_port, root)
        launch(stack, "gate", gate, env, gate_port, root)
        api = Api(f"http://127.0.0.1:{gate_port}", tcp_port, "127.0.0.1")
        stack.callback(api.close)
        game_health = Api(f"http://127.0.0.1:{health_port}")
        if with_uc:
            # Gateway 的 HTTP 健康只代表进程可用。先用只读区服查询验证真实 UC 通道，
            # 避免把首次握手时间算进不可盲重试的注册操作；这里沿用正常生产 deadline。
            probe_uc = Api(api.base_url, tcp_port, "127.0.0.1")
            probe_uc.token = session_token("103", secret)
            wait_for("Gateway is connected to UC before registering the test player",
                     lambda: probe_uc.request("/api/realms", allow_error=True),
                     lambda r: r.get("success") is True, 20)
        authenticate(api, "player", secret, with_uc)
        api.wait_for_empty_player()
        created = api.play(player_pb.CreatePlayerReq(name="Alice"), "create-1")
        def correct_identity(player):
            return (player.player_id == "1_" + api.account_id and player.account_id == api.account_id
                    and player.founding_realm_id == 1 and player.current_realm_id == 1)
        require(correct_identity(created.body.player), "Player identity does not match the authenticated UC account")
        payload = infantry_order(150, 150, 120)
        # 丢掉建角时的连接。在新连接重新鉴权，只凭同账号和原请求 ID 查出持久回执。
        api.close()
        lookup = api.play(receipt_pb.GetReceiptReq(target_request_id="create-1"), "inspect-create")
        require(lookup.request_id == "inspect-create" and lookup.body.target_request_id == "create-1"
                and lookup.body.journal_seq == 1 and lookup.body.receipt.created == created.body,
                "Reauthenticated TCP session did not recover the original creation receipt")
        original = api.play(payload, "march-1")
        require(original.error is None and original.request_id == "march-1",
                "TCP march was not confirmed")
        api.close()
        before = game_health.request("/health")["players"]
        require(before["loads"] == 1 and before["residents"] == 1, "Create and march did not share one active player")
        first.kill()
        first.wait(10)
        # 在整个到达、采集、返城时段保持 GAME 离线。后续只等健康探针，不用登录或查询来触发结算。
        departure = original.body.march
        travel_ms = departure.arrive_at - departure.depart_at
        return_due = departure.arrive_at + 3000 + travel_ms
        time.sleep(max(0, return_due / 1000 - time.time() + 0.1))
        second = launch(stack, "restart", serve, env, health_port, root)
        wait_for("overdue arrival, gathering and return settle without a player request",
                 lambda: game_health.request("/health")["tasks"],
                 lambda tasks: tasks["applied"] >= 3, 15)
        state = wait_for("Gateway reconnects to the original volume",
                         lambda: api.play(player_pb.GetPlayerReq(), allow_error=True),
                         lambda r: r.error is None, 15).body.player
        require(correct_identity(state)
                and api.play(player_pb.CreatePlayerReq(name="Alice"), "create-1") == created,
                "Restart changed the player identity or the original creation receipt")
        require(state.troops["INFANTRY"] == 3000, "Return lost troops or credited them twice")
        require(state.resources["WOOD"] == 600, "Gathered wood was not credited exactly once")
        require(state.active_march_ids == [], "March slot was not released")
        require(api.play(payload, "march-1") == original, "Retry changed the receipt")
        require(api.play(player_pb.GetPlayerReq()).body.player == state, "Retry changed player state")
        after = game_health.request("/health")["players"]
        require(after["loads"] == 1 and after["residents"] == 1, "Restarted GAME did not reuse its reloaded player")
        march_id = original.body.march.march_id
        require(not state.heroes[0].HasField("assigned_march"), "Hero was not released")
        require(api.play(march_pb.GetMarchReq(march_id=march_id)).body.march.state == march_pb.MARCH_STATE_COMPLETED,
                "Original march did not complete")
        print(f"PASS original march {march_id}: 120 troops returned; 600 wood credited once")
        visitor = Api(api.base_url, tcp_port, "127.0.0.1")
        stack.callback(visitor.close)
        authenticate(visitor, "visitor", secret, with_uc)
        stranger_lookup = visitor.play(receipt_pb.GetReceiptReq(target_request_id="march-1"), "visitor-lookup")
        require(not stranger_lookup.body.HasField("receipt") and stranger_lookup.body.target_request_id == "march-1",
                "A different TCP identity could read the owner's receipt")
        visitor.play(player_pb.CreatePlayerReq(name="Bob"))
        require(api.play(player_pb.GetPlayerReq()).body.player == state, "Eviction lost the original committed state")
        directory_stats = game_health.request("/health")["players"]
        require(directory_stats["residents"] == 1 and directory_stats["loads"] == 3,
                "Expected one fresh load per account after displacement")
        print("PASS cold load, memory reuse, and capacity eviction through Gateway")
        outbound = api.play(infantry_order(1150, 1150, 120), "depart-again")
        route_id = outbound.body.march.march_id
        recall = api.play(march_pb.RecallMarchReq(march_id=route_id), "recall-1")
        require(recall.body.march.state == march_pb.MARCH_STATE_RETURNING, "Recall did not start the return")
        require(api.play(march_pb.RecallMarchReq(march_id=route_id), "recall-1") == recall, "Recall retry changed its result")
        wait_for("recalled march returns", lambda: api.play(march_pb.GetMarchReq(march_id=route_id)).body.march,
                 lambda march: march.state == march_pb.MARCH_STATE_COMPLETED, 10)
        final = api.play(player_pb.GetPlayerReq()).body.player
        require(final.troops == state.troops and final.resources == state.resources
                and final.heroes == state.heroes and not final.active_march_ids,
                "Recalled march lost troops, duplicated cargo, or left a reservation")

        attack = infantry_order(120, 120, 650, march_pb.MARCH_MISSION_ATTACK_NPC)
        sent = api.play(attack, "npc-1")
        battle_id = sent.body.march.march_id
        battle_query = report_pb.GetBattleReq(battle_id=battle_id)
        pending = wait_for("NPC input persisted while Worker is offline",
                           lambda: api.play(battle_query, allow_error=True),
                           lambda r: r.error is None and r.body.state == report_pb.REPORT_PENDING, 15).body
        wait_for("GAME records the failed dispatch", lambda: game_health.request("/health")["battles"],
                 lambda stats: stats["failedAttempts"] >= 1, 15)
        reserved = api.play(player_pb.GetPlayerReq()).body.player
        require(reserved.troops["INFANTRY"] == final.troops["INFANTRY"] - 650
                and reserved.heroes[0].assigned_march == battle_id, "Pending battle lost its reservation")
        second.kill()
        second.wait(10)
        launch(stack, "battle-recovery", serve, env, health_port, root)
        recovered = wait_for("pending NPC snapshot survives a GAME crash",
                             lambda: api.play(battle_query, allow_error=True),
                             lambda r: r.error is None, 15).body
        require(recovered == pending, "Restart changed the pending input")
        worker_env = {key: value for key, value in env.items() if key != "SLG_TOKEN_SECRET"}
        launch(stack, "worker", worker, worker_env, battle_port, root, rpc_listening)
        wait_for("independent Worker reply is committed and return finishes without a player command",
                 lambda: game_health.request("/health"),
                 lambda h: h["battles"]["settled"] == 1 and h["tasks"]["applied"] >= 1, 30)
        report = api.play(battle_query).body
        require(report.state == report_pb.REPORT_SETTLED and report.input_hash == pending.input_hash,
                "Settlement used a different snapshot")
        result = report.result
        require(result.outcome == battle_result_pb.BATTLE_ATTACKER_WIN, "Expected the NPC victory fixture to win")
        group, = result.attacker.groups
        after = api.play(player_pb.GetPlayerReq()).body.player
        require(group.initial_count == 650 and group.survivors + group.wounded + group.dead == 650
                and after.troops["INFANTRY"] == reserved.troops["INFANTRY"] + group.survivors
                and after.wounded.get("INFANTRY", 0) == group.wounded
                and after.dead.get("INFANTRY", 0) == group.dead, "NPC report and inventory disagree")
        require(after.resources == {"WOOD": 1100, "FOOD": 500, "GEMS": 10}
                and not after.heroes[0].HasField("assigned_march") and not after.active_march_ids,
                "Victory cargo or reservations disagree after return")
        require(api.play(attack, "npc-1") == sent
                and api.play(player_pb.GetPlayerReq()).body.player == after, "Retry changed a completed battle")
        print(f"PASS NPC {battle_id}: losses match the report and victory cargo was delivered once")
        if unity_probe:
            unity = Api(api.base_url, tcp_port, "127.0.0.1")
            stack.callback(unity.close)
            authenticate(unity, "unity", secret, with_uc)
            check_unity_march(unity_probe, unity, tcp_port)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mysql-it-uc", action="store_true", help="use UC backed by verify-mysql.py's test database")
    parser.add_argument("--unity-probe", type=Path, help="also run a built UnityMarchSmoke.dll on this isolated realm")
    args = parser.parse_args()
    if args.unity_probe and not args.unity_probe.is_file():
        parser.error("Build UnityMarchSmoke before using --unity-probe")
    run(args.mysql_it_uc, args.unity_probe)
