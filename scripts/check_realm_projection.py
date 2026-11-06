"""由 verify-mysql.py 调用，在临时 GAME / Gateway 上核对 MySQL 停机期间的每一笔决定。

Docker 的启停和查询由创建容器的脚本提供，场景本身只持有测试投影用户的凭据。
这样无法通过传入开发库地址或容器名，把故障注入指向正在使用的服务。
"""
import base64
from contextlib import ExitStack
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
from tempfile import TemporaryDirectory
import uuid

from tcp_client import infantry_order, march_pb, player_pb, receipt_pb
from realm_fixture import Api, launch, require, session_token, wait_for

ROOT = Path(__file__).resolve().parent.parent
ACCOUNT = "9007199254740993"  # 超过 JavaScript 精确整数范围，必须全链路按字符串保存。
PLAYER = "1_" + ACCOUNT
EVENTS = ("PLAYER_CREATED", "COMMAND_REJECTED", "MARCH_STARTED",
          "MARCH_GATHER_STARTED", "MARCH_GATHER_FINISHED", "MARCH_COMPLETED")


def caught_up(probe, seq):
    """水位、后台状态和本地水位必须一致；只检查线程存活会漏掉永久停在坏记录处的消费者。"""
    def ready(health):
        projection = health["projection"]
        return (health["journalSeq"] == seq and projection["state"] == "CAUGHT_UP"
                and projection["lastAppliedSeq"] == seq and projection["lag"] == 0)
    wait_for(f"MySQL cursor and GAME agree at sequence {seq}", probe, ready, 60)


def query_snapshot(query, volume_id, player, march, events):
    """核对连续前缀和最终对象版本。单看 COUNT 或最终木材数会漏掉中间少记或重复记的业务事实。"""
    end = len(events)
    checkpoint = query("SELECT volume_id, last_applied_seq FROM projection_checkpoint WHERE realm_id = 1")
    require(checkpoint == f"{volume_id}\t{end}", "Wrong volume or SQL checkpoint")
    event_rows = query("SELECT journal_seq, fact_type FROM projection_event WHERE realm_id = 1 ORDER BY journal_seq")
    require(event_rows.splitlines() == [f"{n}\t{kind}" for n, kind in enumerate(events, 1)],
            "Projection history contains a hole, reordered fact or duplicate")
    rows = query("SELECT object_type,object_id,object_version,journal_seq,state_json FROM projection_object "
                 "WHERE realm_id = 1 ORDER BY object_type,object_id")
    objects = {}
    for row in rows.splitlines():
        kind, identity, version, sequence, payload = row.split("\t", 4)
        objects[(kind, identity)] = (int(version), int(sequence), json.loads(payload))
    # SQL 归档存领域快照；用查询得到的强类型字段逐项对账，包括原卷中的空英雄占用和完成后的空任务。
    expected_player = {"playerId": player.player_id, "accountId": player.account_id,
                       "foundingRealmId": player.founding_realm_id, "currentRealmId": player.current_realm_id,
                       "name": player.name, "troops": dict(player.troops), "activeMarchIds": list(player.active_march_ids),
                       "resources": dict(player.resources), "wounded": dict(player.wounded), "dead": dict(player.dead),
                       "heroes": [{"heroId": h.hero_id, "assignedMarch": h.assigned_march if h.HasField("assigned_march") else None}
                                  for h in player.heroes]}
    expected_march = {"marchId": march.march_id, "playerId": march.player_id, "tier": march.tier,
                      "fromX": march.from_x, "fromY": march.from_y, "targetX": march.target_x, "targetY": march.target_y,
                      "state": march_pb.MarchState.Name(march.state).removeprefix("MARCH_STATE_"),
                      "mission": march_pb.MarchMission.Name(march.mission).removeprefix("MARCH_MISSION_"),
                      "departAt": march.depart_at, "arriveAt": march.arrive_at, "taskId": march.task_id or None,
                      "rulesVersion": march.rules_version, "cargo": dict(march.cargo), "heroIds": list(march.hero_ids),
                      "troops": {march_pb.TroopType.Name(t.type).removeprefix("TROOP_TYPE_"): t.count for t in march.troops}}
    require(objects[("player", PLAYER)] == (player.version, len(EVENTS), expected_player),
            "Projected player version, troops or cargo disagree with GAME")
    # 停库期间由 GAME 决定的状态必须进入正式玩家表；校验账号反查、角色名、对象版本和来源序号。
    typed_player = query("SELECT player_id,founding_realm_id,current_realm_id,name,object_version,journal_seq FROM projection_player "
                         f"WHERE realm_id=1 AND account_id='{ACCOUNT}'")
    require(typed_player == f"{PLAYER}\t1\t1\t{player.name}\t{player.version}\t{len(EVENTS)}",
            "Typed player did not catch up with GAME and its verified object snapshot")
    require(objects[("march", march.march_id)] == (4, len(EVENTS), expected_march),
            "Projected march version or state differs from the authoritative route")
    tasks = [record for (kind, _), record in objects.items() if kind == "task"]
    # 到达占用、采集完成释放，矿点保留第 2 版；矿点变更与任务、货物在同一决定入库。
    require(objects[("resource", "150-150")] == (2, 5, {
        "x": 150, "y": 150, "occupantPlayerId": None, "occupantMarchId": None, "generation": 0,
    }), "Projected mine did not release exactly once after the MySQL outage")
    require(objects[("account-role", ACCOUNT)] == (1, 1, {"playerId": PLAYER}),
            "Account no longer resolves to the character created at J1")
    require(len(objects) == 7 and len(tasks) == 3 and all(version == 2 and task["state"] == "DONE"
            for version, _, task in tasks), "Arrival, gathering or return was lost or credited twice")


def verify_projection_recovery(jdbc, user, password, stop_mysql, start_mysql, query):
    with TemporaryDirectory(prefix="slg-projection-chaos-") as directory, ExitStack() as stack:
        root = Path(directory)
        volume = root / "realm"
        volume.mkdir()
        volume_id = str(uuid.uuid4())
        with ExitStack() as ports:
            sockets = [ports.enter_context(socket.socket()) for _ in range(6)]
            for sock in sockets:
                sock.bind(("127.0.0.1", 0))
            tcp_port, gate_port, health_port, game_rpc, unused_uc, unused_worker = [sock.getsockname()[1] for sock in sockets]

        # 测试会话不依赖 UC。即使将 MySQL 整个关闭，也能单独验证一个已认证玩家的 GAME 路径。
        secret = secrets.token_hex(32)
        localhost = "127.0.0.1"
        env = {k: v for k, v in os.environ.items() if not k.startswith("SLG_")}
        env.update(SLG_BIND=localhost, SLG_TOKEN_SECRET=secret, SLG_GATE_TCP_PORT=str(tcp_port), SLG_GATE_PORT=str(gate_port),
                   SLG_GAME_PORT=str(health_port), SLG_GAME_RPC_PORT=str(game_rpc),
                   SLG_GAME_RPC_TARGET=f"{localhost}:{game_rpc}", SLG_UC_RPC_TARGET=f"{localhost}:{unused_uc}",
                   SLG_BATTLE_RPC_TARGET=f"{localhost}:{unused_worker}")
        game_env = {**env, "SLG_PROJECTION_ENABLED": "true", "SLG_PROJECTION_MAX_LAG": "5", "SLG_PROJECTION_JDBC_URL": jdbc,
                    "SLG_PROJECTION_DB_USER": user, "SLG_PROJECTION_DB_PASSWORD": password}
        java = ["java", "-Xmx160m", "-jar"]
        game = java + [str(ROOT / "slg_game/target/slg_game-1.0-SNAPSHOT.jar")]
        gate = java + [str(ROOT / "slg_gate/target/slg_gate-1.0-SNAPSHOT.jar")]
        identity = [str(volume), "1", volume_id]
        subprocess.run(game + ["volume", "init", *identity], env=env, check=True, timeout=20)
        serve = game + ["realm", "serve", *identity]
        first = launch(stack, "game-before-outage", serve, game_env, health_port, root)
        launch(stack, "gateway", gate, env, gate_port, root)
        api = Api(f"http://{localhost}:{gate_port}", tcp_port, "127.0.0.1")
        stack.callback(api.close)
        api.token = session_token(ACCOUNT, secret)
        health = Api(f"http://{localhost}:{health_port}")

        def probe():
            return health.request("/health")

        api.wait_for_empty_player()
        api.play(player_pb.CreatePlayerReq(name="Alice"), "create")
        caught_up(probe, 1)
        require(query("SELECT last_applied_seq FROM projection_checkpoint WHERE realm_id = 1") == "1",
                "GAME reported projection progress before the SQL transaction committed")
        stop_mysql()
        # 既制造有状态变更的出征，又制造没有对象变更的业务拒绝；二者都必须占据一个有序日志位置。
        payload = infantry_order(150, 150, 120)
        denied = api.play(infantry_order(12, 150, 120), "invalid", allow_error=True)
        require(denied.error.code == "INVALID_TARGET", "Expected a recorded business rejection")
        sent = api.play(payload, "gather")
        require(api.play(payload, "gather") == sent, "Retry changed the departure receipt")
        wait_for("GAME confirms writes while MySQL is stopped and reports the last known SQL cursor", probe,
                 lambda h: h["projection"]["state"] == "RETRYING" and h["journalSeq"] >= 3
                 and h["projection"]["lastAppliedSeq"] == 1 and h["projection"]["lag"] >= 2, 30)
        first.kill()
        first.wait(10)
        second = launch(stack, "game-during-outage", serve, game_env, health_port, root)

        # GAME 从 RocksDB 的任务记录补做采集及返城；不依赖缓存的 SQL 游标或由玩家读请求触发结算。
        wait_for("GAME reopens the original volume and finishes the march before MySQL recovers", probe,
                 lambda h: h["status"] == "UP" and h["volumeId"] == volume_id and h["journalSeq"] == len(EVENTS)
                 and h["projection"]["state"] == "RETRYING", 40)
        player = wait_for("Gateway reads the authoritative player before MySQL recovery",
                          lambda: api.play(player_pb.GetPlayerReq(), allow_error=True),
                          lambda reply: reply.error is None, 15).body.player
        require(player.troops["INFANTRY"] == 3000 and player.resources["WOOD"] == 600
                and not player.active_march_ids and not player.heroes[0].HasField("assigned_march"),
                "The offline return lost troops, duplicated cargo or left a hero assigned")
        march = api.play(march_pb.GetMarchReq(march_id=sent.body.march.march_id)).body.march
        require(march.state == march_pb.MARCH_STATE_COMPLETED, "GAME did not finish this march")

        # SQL 持久游标在 1，当前已经有六条决定。限额 5 的目的，是停止放大积压，又保留读和原回执。
        # 这里使用一个确定会得到 PLAYER_EXISTS 的新建角请求；限流本身不能写回执，否则恢复后原 ID 无法再执行。
        resume_body = player_pb.CreatePlayerReq(name="AlreadyCreated")
        limited = api.play(resume_body, "after-outage", allow_error=True)
        require(limited.error.code == "PROJECTION_BACKLOG"
                and api.play(payload, "gather") == sent and probe()["journalSeq"] == len(EVENTS),
                "Lag limit must reject only new writes and leave their request IDs reusable")

        # GAME 已强杀重启，SQL 还停在 J1。用只读请求区分三种结果：出征成功、业务已拒绝、尚无持久决定。
        # 查询自己的 requestId 与待查 ID 分开；完整比对内层响应，避免用已经返城的最新状态替代出征回执。
        recorded = {}
        for request_id, sequence in (("gather", 3), ("invalid", 2), ("after-outage", 0)):
            lookup_id = "inspect-" + request_id
            lookup = api.play(receipt_pb.GetReceiptReq(target_request_id=request_id), lookup_id)
            body = lookup.body
            require(lookup.request_id == lookup_id and body.target_request_id == request_id and body.journal_seq == sequence,
                    "Offline receipt identity or journal position differs for " + request_id)
            if body.HasField("receipt"):
                recorded[sequence] = body.receipt
            if request_id == "gather":
                require(body.receipt.started == sent.body, "Recovered departure differs from the original response")
            elif request_id == "invalid":
                require(body.receipt.error == denied.error, "Recovered rejection differs from the original error")
            else:
                require(not body.HasField("receipt"), "Deferred write unexpectedly has a receipt")
        require(probe()["journalSeq"] == len(EVENTS), "Receipt lookups must not append decisions")
        print("PASS receipt lookup after GAME restart with MySQL offline: J2 rejection, J3 departure, "
              "and no receipt for the deferred write; journal stays at J6", flush=True)

        start_mysql()
        caught_up(probe, len(EVENTS))
        query_snapshot(query, volume_id, player, march, EVENTS)
        # J2 的拒绝和 J3 的出征成功均需逐项投影到 SQL；从 MySQL 的 JSON 字符串解出同一条强类型决定。
        for sequence, original in recorded.items():
            encoded = json.loads(query(f"SELECT result_json FROM projection_event WHERE realm_id=1 AND journal_seq={sequence}"))
            projected = receipt_pb.WriteReceipt.FromString(base64.b64decode(encoded, validate=True))
            require(projected == original, "SQL receipt differs from the recovered volume at J" + str(sequence))
        resumed = api.play(resume_body, "after-outage", allow_error=True)
        require(resumed.error.code == "PLAYER_EXISTS", "Original ID was not admitted after catch-up")
        final_events = (*EVENTS, "COMMAND_REJECTED")
        caught_up(probe, len(final_events))
        # 再次丢弃 GAME 的全部内存。完成过的消费游标必须从 SQL 找回，原请求也必须只读回执。
        second.kill()
        second.wait(10)
        launch(stack, "game-after-catchup", serve, game_env, health_port, root)
        caught_up(probe, len(final_events))
        result = wait_for("Gateway reconnects and retry returns the original committed receipt",
                          lambda: api.play(payload, "gather", allow_error=True),
                          lambda reply: reply.error is None, 15)
        require(result == sent and api.play(player_pb.GetPlayerReq()).body.player == player and probe()["journalSeq"] == len(final_events),
                "Reopening or retry changed the journal, player or receipt")
        query_snapshot(query, volume_id, player, march, final_events)
        print(f"PASS MySQL stopped at 1, GAME recovered offline, SQL caught up to {len(final_events)}; "
              "one award of 600 wood and 120 troops returned, including after restart and retry", flush=True)
