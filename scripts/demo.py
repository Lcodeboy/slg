#!/usr/bin/env python3
"""通过开发网关创建独立演示账号，发起一次采集行军，等待返城并校验幂等回执。"""
import argparse
import ssl
import uuid
from contextlib import closing

from tcp_client import infantry_order, march_pb, player_pb
from realm_fixture import Api, require, wait_for


def run(api):
    name = api.create_account("march")
    player = api.play(player_pb.CreatePlayerReq(name=name)).body.player
    troops = player.troops["INFANTRY"]
    before = player.resources["WOOD"]
    request_id = str(uuid.uuid4())
    payload = infantry_order(150, 150, 120, march_pb.MARCH_MISSION_GATHER)
    departure = api.play(payload, request_id)
    march_id = departure.body.march.march_id
    wait_for("gathering march returns", lambda: api.play(march_pb.GetMarchReq(march_id=march_id)).body.march,
             lambda march: march.state == march_pb.MARCH_STATE_COMPLETED, 45)
    state = api.play(player_pb.GetPlayerReq()).body.player
    require(state.troops["INFANTRY"] == troops and state.resources["WOOD"] > before,
            "Return did not restore the dispatched troops and grant gathered wood")
    require(api.play(payload, request_id) == departure, "Retry changed the original result")
    require(api.play(player_pb.GetPlayerReq()).body.player == state, "Retry applied the march a second time")
    print("PASS", march_id, "returned with", state.resources["WOOD"] - before, "wood")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--tcp-host", help="Unity TCP listener hostname (defaults to the HTTP host)")
    parser.add_argument("--tcp-port", type=int, help="Unity TCP listener port (default 8090)")
    parser.add_argument("--tcp-ca", help="Trust this CA for TCP TLS (otherwise use system CAs with HTTPS)")
    args = parser.parse_args()
    tls = ssl.create_default_context(cafile=args.tcp_ca) if args.tcp_ca or args.base_url.startswith("https://") else None
    with closing(Api(args.base_url, args.tcp_port, args.tcp_host, tls)) as api:
        run(api)
