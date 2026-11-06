"""在一次性 MySQL 和隔离区服上，从真实网关并发派两支队伍争同一座矿。"""
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
from tempfile import TemporaryDirectory
from threading import Barrier
import time
import uuid

from tcp_client import infantry_order, march_pb, player_pb
from realm_fixture import Api, launch, require, session_token, wait_for

ROOT = Path(__file__).resolve().parent.parent
REALM = 2  # 同一测试数据库中，停库追赶使用 1 区；本场景单独建卷，从空游标开始。


def check_resource(query, version, player_id=None, march=None):
    row = query("SELECT object_version,journal_seq,state_json FROM projection_object "
                "WHERE realm_id=2 AND object_type='resource' AND object_id='150-150'")
    require(row, "Expected a committed snapshot of the contested mine")
    actual_version, sequence, payload = row.split("\t", 2)
    require(int(actual_version) == version and json.loads(payload) == {
        "x": 150, "y": 150, "occupantPlayerId": player_id, "occupantMarchId": march,
        "generation": 2 if march else 0,
    }, "The SQL mine version, owner, or owning task generation disagrees with the march")
    # 按坐标取当前矿主，再按玩家取其占矿列表。两次查询都走正式列，同时检查业务版本与来源序号。
    typed = query("SELECT occupant_player_id,occupant_march_id,generation,object_version,journal_seq "
                  "FROM projection_resource_site WHERE realm_id=2 AND x=150 AND y=150")
    require(typed == f"{player_id or 'NULL'}\t{march or 'NULL'}\t{2 if march else 0}\t{version}\t{sequence}",
            "Typed site and archived object disagree at the same committed position")
    owned = query("SELECT occupant_player_id,x,y FROM projection_resource_site WHERE realm_id=2 "
                  "AND occupant_player_id IN ('2_101','2_102') ORDER BY x,y")
    require(owned == (f"{player_id}\t150\t150" if player_id else ""), "Owner query missed or retained a site")


def verify_mine_contention(jdbc, user, password, query):
    """调用者提供本轮测试库的只读查询；子进程只能得到独立的投影账号和新卷路径。"""
    with TemporaryDirectory(prefix="slg-contested-mine-") as tmp, ExitStack() as stack:
        root = Path(tmp)
        volume = root / "realm"
        volume.mkdir()
        volume_id = str(uuid.uuid4())
        with ExitStack() as ports:
            sockets = [ports.enter_context(socket.socket()) for _ in range(6)]
            for sock in sockets:
                sock.bind(("127.0.0.1", 0))
            tcp_port, gate_port, rpc_port, health_port, unused_uc, unused_worker = [s.getsockname()[1] for s in sockets]
        secret = secrets.token_hex(32)
        env = {k: v for k, v in os.environ.items() if not k.startswith("SLG_")}
        env.update(SLG_BIND="127.0.0.1", SLG_TOKEN_SECRET=secret, SLG_GATE_TCP_PORT=str(tcp_port), SLG_GATE_PORT=str(gate_port),
                   SLG_GAME_PORT=str(health_port), SLG_GAME_RPC_PORT=str(rpc_port),
                   SLG_GAME_RPC_TARGET=f"127.0.0.1:{rpc_port}", SLG_UC_RPC_TARGET=f"127.0.0.1:{unused_uc}",
                   SLG_BATTLE_RPC_TARGET=f"127.0.0.1:{unused_worker}")
        game_env = {**env, "SLG_PROJECTION_ENABLED": "true", "SLG_PROJECTION_JDBC_URL": jdbc,
                    "SLG_PROJECTION_DB_USER": user, "SLG_PROJECTION_DB_PASSWORD": password}
        java = ["java", "-Xmx160m", "-jar"]
        game = java + [str(ROOT / "slg_game/target/slg_game-1.0-SNAPSHOT.jar")]
        gate = java + [str(ROOT / "slg_gate/target/slg_gate-1.0-SNAPSHOT.jar")]
        identity = [str(volume), str(REALM), volume_id]
        subprocess.run(game + ["volume", "init", *identity], cwd=root, env=env, check=True, timeout=20)
        serve = game + ["realm", "serve", *identity]
        original = launch(stack, "mine-game", serve, game_env, health_port, root)
        launch(stack, "mine-gateway", gate, env, gate_port, root)
        health = Api(f"http://127.0.0.1:{health_port}")
        accounts = ("101", "102")
        roles = tuple(f"{REALM}_{account}" for account in accounts)
        amounts = (120, 80)
        players = [Api(f"http://127.0.0.1:{gate_port}", tcp_port, "127.0.0.1") for _ in accounts]
        payloads = [infantry_order(150, 150, count) for count in amounts]
        for i, client in enumerate(players):
            stack.callback(client.close)
            client.token = session_token(accounts[i], secret)
            client.wait_for_empty_player()
            created = client.play(player_pb.CreatePlayerReq(name=("Alice", "Bob")[i]), "create").body.player
            require(created.player_id == roles[i], "Wrong role for the authenticated account")

        def await_seq(sequence):
            wait_for(f"contested realm and MySQL reach J{sequence}",
                     lambda: health.request("/health"),
                     lambda h: h["volumeId"] == volume_id and h["journalSeq"] == sequence
                     and h["projection"]["state"] == "CAUGHT_UP" and h["projection"]["lastAppliedSeq"] == sequence, 25)

        def check_return(i, wood):
            state = players[i].play(player_pb.GetPlayerReq()).body.player
            require(state.resources["WOOD"] == wood and state.troops["INFANTRY"] == 3000
                    and state.active_march_ids == [] and not state.heroes[0].HasField("assigned_march"),
                    "Returned troops, released hero or gathering reward disagree for " + accounts[i])

        ready = Barrier(2)

        def depart(i):
            ready.wait(timeout=5)  # 两个已鉴权的 TCP 会话同时出征，不假设哪个玩家在目标队列里排到第一。
            return players[i].play(payloads[i], "contest")

        with ThreadPoolExecutor(max_workers=2) as workers:
            receipts = list(workers.map(depart, range(2)))
        marches = [receipt.body.march.march_id for receipt in receipts]
        require(marches[0] != marches[1], "Distinct players reused a march identity")
        # J1..J4 建角、出征，J5/J6 是两支队到达的决定。只观察已投影到这两个决定的短窗，
        # 再同时检查真实行军状态和进程退出时间；超窗直接失败，不能把释放矿点后的强杀算作成功。
        await_seq(6)
        on_map = [p.play(march_pb.GetMarchReq(march_id=m)).body.march for p, m in zip(players, marches)]
        states = [m.state for m in on_map]
        require(sorted(states) == [march_pb.MARCH_STATE_GATHERING, march_pb.MARCH_STATE_RETURNING], "Expected one occupied mine and one empty return")
        winner = states.index(march_pb.MARCH_STATE_GATHERING)
        loser = 1 - winner
        require(not on_map[loser].cargo, "A denied march carried a reward")
        original.kill()
        require(original.wait(10) != 0 and time.time() * 1000 < on_map[winner].arrive_at,
                "GAME must have been killed while the winner still owned the mine")
        # GAME 已无法再写；直接读刚才已获确认的 SQL 边界，核对唯一占用者与任务代次。
        check_resource(query, 1, roles[winner], marches[winner])
        print(f"PASS killed GAME at J6 during mine occupation: owner={roles[winner]}, other march returning empty",
              flush=True)

        launch(stack, "mine-recovered", serve, game_env, health_port, root)
        # 不发送客户端命令推动状态。两个到达已完成，剩余三个后台任务是采完与两次返城。
        await_seq(9)
        wait_for("Gateway reconnects to the contested realm", lambda: players[0].play(player_pb.GetPlayerReq(), allow_error=True),
                 lambda r: r.error is None, 15)
        check_resource(query, 2)
        for i, client in enumerate(players):
            require(client.play(payloads[i], "contest") == receipts[i],
                    "Retry changed an original march receipt")
            require(client.play(march_pb.GetMarchReq(march_id=marches[i])).body.march.state == march_pb.MARCH_STATE_COMPLETED,
                    "An original march did not return")
            check_return(i, amounts[winner] * 5 if i == winner else 0)
        # 让原败者再派新队伍占同一矿，避免仅凭终态 FREE 推断下一位玩家确实可采集。
        replacement = players[loser].play(payloads[loser], "contest-again")
        require(replacement.body.march.march_id not in marches, "New march reused an old receipt")
        await_seq(13)
        check_resource(query, 4)
        check_return(winner, amounts[winner] * 5)
        check_return(loser, amounts[loser] * 5)
        # 每支队的任务必须落在自己的原始行军上；总奖励一致也可能掩盖两队串账。
        events = query("SELECT journal_seq,fact_type,fact_json FROM projection_event WHERE realm_id=2 "
                       "ORDER BY journal_seq").splitlines()
        timer_events = {}
        for seq, row in enumerate(events, 1):
            index, event, fact = row.split("\t", 2)
            require(int(index) == seq, "Missing decision in the projected realm history")
            if event not in ("PLAYER_CREATED", "MARCH_STARTED"):
                march = json.loads(fact)["result"]["marchId"]
                timer_events.setdefault(march, []).append(event)
        success = ["MARCH_GATHER_STARTED", "MARCH_GATHER_FINISHED", "MARCH_COMPLETED"]
        require(len(events) == 13 and timer_events == {
            marches[winner]: success, marches[loser]: ["MARCH_TARGET_OCCUPIED", "MARCH_COMPLETED"],
            replacement.body.march.march_id: success,
        }, "Duplicate, missing or misattributed mine decision")
        require(query("SELECT volume_id,last_applied_seq FROM projection_checkpoint WHERE realm_id=2")
                == f"{volume_id}\t13", "MySQL checkpoint does not belong to the recovered realm")
        print("PASS both armies returned once, one reward for the contested mine; "
              "former loser gathered after release, SQL mine version=4, journal=MySQL=13", flush=True)
