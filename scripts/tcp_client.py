"""验收和压测共用的 Unity TCP 线协议。消息由仓库中的 .proto 生成，socket 只负责有界分帧。"""
import asyncio
from dataclasses import dataclass
import ipaddress
from pathlib import Path
import socket
import struct
import sys
import time

# Maven 将当前协议一次生成给 GAME、Unity 和 Python。使用项目生成目录，避免从环境里加载其他版本。
_GENERATED = Path(__file__).resolve().parents[1] / "slg_rpc/target/generated-sources/python"
if not _GENERATED.is_dir():
    raise ImportError("Generate the game protocol first: ./mvnw -pl slg_rpc -am compile")
sys.path.insert(0, str(_GENERATED))
try:
    from google.protobuf.message import Message
    from google.protobuf.message_factory import GetMessageClass
    from slg_gateway_pb2 import GatewayFrame
    from slg_gameplay_options_pb2 import msg_id
    import slg_battle_result_pb2 as battle_result_pb
    import slg_march_pb2 as march_pb
    import slg_player_pb2 as player_pb
    import slg_receipt_pb2 as receipt_pb
    import slg_report_pb2 as report_pb
    from slg_rpc_pb2 import BusinessError
except ModuleNotFoundError as error:
    if error.name in {"google", "google.protobuf"}:
        raise ImportError("Install the TCP client runtime: python3 -m pip install -r scripts/requirements.txt") from error
    raise

VERSION = 2
MAX_REQUEST = 65_536
MAX_RESPONSE = 1_048_592
LENGTH = struct.Struct("!I")
DEFAULT_TCP_PORT = 8090
# protoc 描述符提供操作号、请求和响应类型。测试与压测接受真实请求对象，也走公开的 TCP 协议。
_METHODS = [method for module in (march_pb, player_pb, receipt_pb, report_pb)
            for service in module.DESCRIPTOR.services_by_name.values() for method in service.methods]
_BY_REQUEST = {method.input_type: method for method in _METHODS}
_BY_ID = {method.GetOptions().Extensions[msg_id]: method for method in _METHODS}


@dataclass(frozen=True)
class CallResult:
    status: int
    request_id: str
    body: Message = None
    error: BusinessError = None


def infantry_order(x, y, count, mission=None):
    """常用的验收编队。默认英雄与显式空英雄列表由调用者在强类型消息上选择。"""
    order = march_pb.StartMarchReq(target_x=x, target_y=y)
    order.troops.add(type=march_pb.TROOP_TYPE_INFANTRY, count=count)
    if mission is not None:
        order.mission = mission
    return order


def auth_frame(token):
    frame = GatewayFrame(version=VERSION)
    frame.auth.token = token
    return frame


def gameplay_frame(request_id, request):
    method = _BY_REQUEST[request.DESCRIPTOR]
    frame = GatewayFrame(version=VERSION)
    frame.play_request.msg_id = method.GetOptions().Extensions[msg_id]
    frame.play_request.request_id = request_id
    frame.play_request.body = request.SerializeToString()
    return frame


def heartbeat(kind, nonce):
    frame = GatewayFrame(version=VERSION)
    getattr(frame, kind).nonce = nonce
    return frame


def encode_frame(frame):
    payload = frame.SerializeToString()
    if not 0 < len(payload) <= MAX_REQUEST:
        raise ValueError("Invalid outgoing gateway frame size")
    return LENGTH.pack(len(payload)) + payload


def response_size(header):
    size, = LENGTH.unpack(header)
    if not 0 < size <= MAX_RESPONSE:
        raise ValueError("Invalid incoming gateway frame size")
    return size


def decode_frame(payload):
    frame = GatewayFrame.FromString(payload)
    if frame.version != VERSION or frame.WhichOneof("body") is None:
        raise ValueError("Invalid gateway frame version or body")
    if frame.HasField("error"):
        raise ConnectionError("Gateway protocol error: " + frame.error.code)
    return frame


def read_result(frame, request):
    """写后断线或关联标识出错时，交上层用原 ID 查询。此处只接受当前操作声明的响应。"""
    expected, reply = request.play_request, frame.play_reply
    if (not frame.HasField("play_reply") or reply.msg_id != expected.msg_id
            or reply.request_id != expected.request_id or not 200 <= reply.status <= 599):
        raise ValueError("Unexpected gameplay reply")
    outcome = reply.WhichOneof("outcome")
    if outcome == "body" and reply.status == 200:
        parser = GetMessageClass(_BY_ID[reply.msg_id].output_type)
        return CallResult(reply.status, reply.request_id, body=parser.FromString(reply.body))
    if outcome == "error" and reply.error.code.strip():
        return CallResult(reply.status, reply.request_id, error=reply.error)
    raise ValueError("Incomplete gameplay reply")


class TcpClient:
    """一个账号的一条同步连接；测试断线后由调用者重新鉴权、查询原 requestId。"""
    def __init__(self, host, port, token, tls=None, timeout=15):
        if tls is None and host != "localhost":
            try:
                loopback = ipaddress.ip_address(host).is_loopback
            except ValueError:
                loopback = False
            if not loopback:
                raise ValueError("TCP session tokens require TLS outside loopback")
        self.timeout = timeout
        self.socket = socket.create_connection((host, port), timeout=timeout)
        try:
            if tls is not None:
                self.socket = tls.wrap_socket(self.socket, server_hostname=host)
            self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.send(auth_frame(token))
            authenticated = self.receive()
            if not authenticated.HasField("auth_ok") or not authenticated.auth_ok.account_id:
                raise ValueError("Expected authenticated gateway session")
            self.account_id = authenticated.auth_ok.account_id
        except BaseException:
            self.close()
            raise

    def send(self, frame):
        self.socket.settimeout(self.timeout)
        self.socket.sendall(encode_frame(frame))

    def _read_bytes(self, count, deadline):
        data = bytearray()
        while len(data) < count:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("TCP gateway read deadline exceeded")
            self.socket.settimeout(remaining)
            chunk = self.socket.recv(count - len(data))
            if not chunk:
                raise ConnectionError("TCP gateway disconnected")
            data.extend(chunk)
        return bytes(data)

    def receive(self):
        # 整条回复共用截止时间；半包、慢速连续字节或心跳不应无限延长一个业务请求。
        deadline = time.monotonic() + self.timeout
        while True:
            size = response_size(self._read_bytes(LENGTH.size, deadline))
            frame = decode_frame(self._read_bytes(size, deadline))
            if not frame.HasField("ping"):
                return frame
            self.send(heartbeat("pong", frame.ping.nonce))

    def play(self, request_id, message):
        request = gameplay_frame(request_id, message)
        self.send(request)
        return read_result(self.receive(), request)

    def close(self):
        self.socket.close()


class AsyncTcpClient:
    """压测使用一条已认证长连和一个读取协程。空闲连接也消费心跳，断连及时从持有数扣除。"""
    def __init__(self, host, port, index, token):
        self.host, self.port, self.account_index, self.token = host, port, index, token
        self.reader = self.writer = self.pump = self.pending = None
        self.request = None
        self.alive = False

    async def receive(self):
        size = response_size(await self.reader.readexactly(LENGTH.size))
        return decode_frame(await self.reader.readexactly(size))

    async def connect(self):
        # 压测进程总在本机隔离卷和回环地址上运行；调用者对建连批次设置整体超时。
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        self.writer.write(encode_frame(auth_frame(self.token)))
        await self.writer.drain()
        if not (await self.receive()).HasField("auth_ok"):
            raise ValueError("TCP authentication rejected")
        self.alive = True
        self.pump = asyncio.create_task(self.read_loop())

    async def read_loop(self):
        try:
            while True:
                # 网关写空闲 20s 发 ping。静默或永远凑不齐的半包必须使空闲会话失效。
                frame = await asyncio.wait_for(self.receive(), 65)
                if frame.HasField("ping"):
                    self.writer.write(encode_frame(heartbeat("pong", frame.ping.nonce)))
                    await asyncio.wait_for(self.writer.drain(), 5)
                elif self.pending and not self.pending.done():
                    self.pending.set_result(read_result(frame, self.request))
                else:
                    raise ValueError("Unsolicited gateway response")
        except (Exception, asyncio.CancelledError):
            self.alive = False
            if self.pending and not self.pending.done():
                self.pending.set_exception(ConnectionError("TCP gateway disconnected"))
        finally:
            self.writer.close()

    async def play(self, request_id, message, attempt=None):
        if not self.alive or self.pending is not None:
            raise ConnectionError("TCP client is unavailable or already has an outstanding command")
        self.request = gameplay_frame(request_id, message)
        data = encode_frame(self.request)
        self.pending = asyncio.get_running_loop().create_future()
        try:
            # write 后即使 drain/读回执失败也可能已提交，不能计为未发出。
            if attempt is not None:
                attempt.sent = True
            self.writer.write(data)
            await self.writer.drain()
            return await self.pending
        finally:
            self.request = self.pending = None

    async def close(self):
        self.alive = False
        if self.pump:
            self.pump.cancel()
            await asyncio.gather(self.pump, return_exceptions=True)
        if self.writer:
            self.writer.close()
            try:
                await asyncio.wait_for(self.writer.wait_closed(), .5)
            except (OSError, asyncio.TimeoutError):
                pass
