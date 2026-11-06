"""Measurement accounting and resource protections; no Docker or pressure run is started."""
import asyncio
import json
from pathlib import Path
import tempfile
import unittest

from tcp_client import (AsyncTcpClient, BusinessError, CallResult, GatewayFrame, LENGTH, MAX_RESPONSE, VERSION,
                        encode_frame, gameplay_frame, heartbeat, march_pb, player_pb, read_result)
from load_test import Attempt, ROOT, Stats, classify, private_json, run_phase, workload


class LoadAccountingTests(unittest.TestCase):
    def test_admission_rejections_are_not_success_and_uncertain_commits_stay_unknown(self):
        for status, code in ((429,"RATE_LIMITED"),(503,"SERVER_BUSY"),(503,"UPSTREAM_BUSY"),(504,"DEADLINE_EXCEEDED"),(200,"QUEUE_BUSY")):
            response = CallResult(status, "one", error=BusinessError(code=code))
            self.assertEqual(("reject",code), classify(response, "one"))
        for code in ("STORAGE_UNAVAILABLE","RESULT_UNKNOWN","UPSTREAM_TIMEOUT_UNKNOWN","UPSTREAM_UNAVAILABLE","UPSTREAM_UNAVAILABLE_UNKNOWN"):
            for status in (503, 200):
                response = CallResult(status, "one", error=BusinessError(code=code))
                self.assertEqual(("unknown",code), classify(response, "one"))
        self.assertEqual("unknown", classify(CallResult(200, "other", body=player_pb.GetPlayerRsp()), "one")[0])

    def test_outcomes_partition_offered_and_only_true_mutations_count_as_writes(self):
        stats = Stats(offered=4)
        stats.outcome(Attempt(True),march_pb.StartMarchReq.DESCRIPTOR.name,"success","OK",12,4)
        stats.outcome(Attempt(True),march_pb.StartMarchReq.DESCRIPTOR.name,"reject","QUEUE_BUSY",8,2)
        stats.outcome(Attempt(True),march_pb.StartMarchReq.DESCRIPTOR.name,"unknown","TIMEOUT",1000,3)
        stats.outcome(Attempt(False),march_pb.StartMarchReq.DESCRIPTOR.name,"unknown","CONNECT_FAILED",2,0)
        report = stats.report(1,2)
        self.assertTrue(report["accounting_valid"])
        self.assertEqual((1,1,1,1),(report["success"],report["reject"],report["unknown"],report["not_sent"]))
        self.assertEqual(1,report["write_success"])
        self.assertEqual(1,report["business_rejected"])
        self.assertEqual(12,report["success_latency_from_scheduled_ms"]["p99"])

    def test_mix_issues_one_march_per_five_commands(self):
        self.assertEqual(4, sum(isinstance(workload(i, True), march_pb.StartMarchReq) for i in range(20)))
        self.assertTrue(all(isinstance(workload(i, False), player_pb.GetPlayerReq) for i in range(20)))

    def test_march_tool_encodes_explicit_empty_selection_and_zero_coordinates(self):
        for message, has_heroes, has_x in ((march_pb.StartMarchReq(), False, False),
                                           (march_pb.StartMarchReq(heroes=march_pb.HeroSelection(), target_x=0), True, True)):
            message.troops.add(type=march_pb.TROOP_TYPE_INFANTRY, count=120)
            frame = gameplay_frame("march-1", message)
            self.assertTrue(frame.HasField("play_request"))
            request = march_pb.StartMarchReq.FromString(frame.play_request.body)
            self.assertEqual((has_heroes, has_x), (request.HasField("heroes"), request.HasField("target_x")))
            self.assertEqual(120, request.troops[0].count)

    def test_march_tool_matches_both_ids_and_requires_a_reply_outcome(self):
        request = gameplay_frame("march-2", march_pb.StartMarchReq())
        frame = GatewayFrame(version=VERSION)
        reply = frame.play_reply
        reply.msg_id, reply.request_id, reply.status = request.play_request.msg_id, "march-2", 200
        with self.assertRaises(ValueError):
            read_result(frame, request)
        reply.error.code = "INVALID_ARGUMENT"
        self.assertEqual("INVALID_ARGUMENT", read_result(frame, request).error.code)
        reply.request_id = "other"
        with self.assertRaises(ValueError):
            read_result(frame, request)
        reply.request_id, reply.msg_id = "march-2", reply.msg_id + 1
        with self.assertRaises(ValueError):
            read_result(frame, request)

    def test_report_never_overwrites_runtime_configuration_or_existing_files(self):
        with self.assertRaises(ValueError):
            private_json(ROOT/"data/mysql.env",{})
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"report.json"
            private_json(path,{"ok":True})
            self.assertEqual(0o600,path.stat().st_mode & 0o777)
            with self.assertRaises(ValueError):
                private_json(path,{"ok":False})
            self.assertEqual({"ok":True},json.loads(path.read_text()))


class FakeClient:
    def __init__(self, delay):
        self.delay=delay
        self.account_index=0

    async def play(self, request_id, message, attempt):
        attempt.sent=True
        await asyncio.sleep(self.delay)
        return CallResult(200, request_id, body=player_pb.GetPlayerRsp())

    async def close(self):
        pass


class MemoryWriter:
    def __init__(self):
        self.bytes=bytearray()
        self.closed=False
    def write(self,value): self.bytes.extend(value)
    async def drain(self): pass
    def is_closing(self): return self.closed
    def close(self): self.closed=True
    async def wait_closed(self): pass


class LoadAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_slow_responses_do_not_reduce_offered_rate_or_make_the_queue_unbounded(self):
        report=await run_phase([FakeClient(.08)],1000,.04,2,.5,1,True,"bounded")
        self.assertEqual(40,report["offered"])
        self.assertLessEqual(report["max_client_queue"],2)
        self.assertGreater(report["not_sent"],30)
        self.assertTrue(report["accounting_valid"])
        self.assertFalse(report["target_phase_pass"])
        self.assertEqual(0,report["steady_window_success_per_second"])

    async def test_sent_timeouts_are_unknown_not_rejections(self):
        report=await run_phase([FakeClient(1)],100,.03,2,.01,1,True,"timeout")
        self.assertGreater(report["unknown"],0)
        self.assertEqual(0,report["reject"])
        self.assertTrue(report["accounting_valid"])

    async def test_tcp_matches_result_and_echoes_ping_when_frames_arrive_together(self):
        client = AsyncTcpClient("127.0.0.1", 1, 0, "token")
        client.reader, client.writer, client.alive = asyncio.StreamReader(), MemoryWriter(), True
        client.pump = asyncio.create_task(client.read_loop())
        message = player_pb.GetPlayerReq()
        sample = gameplay_frame("one", message)
        data = player_pb.GetPlayerRsp(player=player_pb.PlayerView(version=2**63-1))
        result = GatewayFrame(version=VERSION)
        result.play_reply.request_id = "one"
        result.play_reply.msg_id = sample.play_request.msg_id
        result.play_reply.status = 200
        result.play_reply.body = data.SerializeToString()
        received = encode_frame(heartbeat("ping", 2**64-1)) + encode_frame(result)
        client.reader.feed_data(received[:2])
        asyncio.get_running_loop().call_soon(client.reader.feed_data, received[2:])
        try:
            attempt = Attempt()
            response = await asyncio.wait_for(client.play("one", message, attempt), 1)
            self.assertTrue(attempt.sent)
            self.assertEqual((200, data), (response.status, response.body))
            self.assertEqual(encode_frame(sample) + encode_frame(heartbeat("pong", 2**64-1)), client.writer.bytes)
        finally:
            await client.close()

    async def test_tcp_rejects_overlong_prefix_and_partial_frame(self):
        for invalid, expected in ((LENGTH.pack(MAX_RESPONSE + 1), ValueError), (LENGTH.pack(0), ValueError),
                                  (LENGTH.pack(10) + b"ab", asyncio.IncompleteReadError)):
            with self.subTest(invalid=invalid):
                client = AsyncTcpClient("127.0.0.1", 1, 0, "token")
                client.reader = asyncio.StreamReader()
                client.reader.feed_data(invalid)
                client.reader.feed_eof()
                with self.assertRaises(expected):
                    await asyncio.wait_for(client.receive(), .1)


if __name__ == "__main__":
    unittest.main()
