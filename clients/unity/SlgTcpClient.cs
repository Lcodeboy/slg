using System;
using System.Collections.Generic;
using System.IO;
using System.Globalization;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Slg.Rpc.V1;

namespace Slg.Unity
{
    /// <summary>网关已经明确结束会话，例如令牌失效时 Code 为 UNAUTHORIZED。</summary>
    public sealed class GatewayConnectionException : IOException
    {
        public string Code { get; }
        internal GatewayConnectionException(BusinessError error) : base(error.Message) => Code = error.Code;
    }

    /// <summary>
    /// 指定请求的应用错误。Status 是应用结果分类，不是 HTTP 状态码；Code 是具体业务原因。
    /// 结果未知时，调用方保存原 requestId 和原请求内容，重连后查回执或原样重试。
    /// </summary>
    public sealed class GameplayException : Exception
    {
        public string RequestId { get; }
        public string Code { get; }
        public uint Status { get; }
        internal GameplayException(WireFrame frame, BusinessError error) : base(error.Message)
        { RequestId = frame.RequestId; Code = error.Code; Status = frame.Status; }
    }

    /// <summary>
    /// 一条原生 Unity TCP/TLS 连接，只有一个读取循环和一个有序写入口。
    /// 通用消息头负责关联，生成的协议注册表负责类型与路由；GAME、地图查询、地图控制各有独立的有界在途名额。
    /// </summary>
    public sealed partial class SlgTcpClient : IDisposable, WireRoutes.IPushHandlers
    {
        private enum Lane { Gateway, Game, MapQuery, MapControl }
        private sealed class PendingCall
        {
            internal readonly uint MsgId;
            internal readonly Lane Lane;
            internal readonly TaskCompletionSource<WireFrame> Completion =
                new TaskCompletionSource<WireFrame>(TaskCreationOptions.RunContinuationsAsynchronously);
            internal PendingCall(uint msgId, Lane lane) { MsgId = msgId; Lane = lane; }
        }
        private readonly TcpClient socket = new TcpClient { NoDelay = true };
        private readonly SemaphoreSlim writer = new SemaphoreSlim(1, 1);
        private readonly object state = new object();
        private readonly Dictionary<string, PendingCall> pending = new Dictionary<string, PendingCall>();
        private Stream stream;
        private Exception closeReason;
        public string AccountId { get; private set; }
        public long ExpiresAt { get; private set; }
        /// <summary>本次认证绑定的 GAME 进程身份，仅供诊断；重启后变化，客户端不能自行选择或上报。</summary>
        public string GameInstance { get; private set; }
        /// <summary>GAME 确认的连接代次，仅供诊断；新连接成功后旧连接的命令会被权威入口拒绝。</summary>
        public long SessionGeneration { get; private set; }

        private SlgTcpClient() {}

        /// <summary>
        /// 校验服务端证书链和 host，再发送可复用的 UC 登录令牌；AUTH_OK 表示 GAME 已建立本连接绑定，
        /// 不只是 Gateway 本地验签。每次重连创建新客户端，调用方显式选择目标网关和鉴权截止时间。
        /// 本方法不自动重放写请求、不刷新令牌，也不把旧连接的地图队列迁移到新连接。
        /// </summary>
        public static Task<SlgTcpClient> ConnectAsync(string host, int port, string token, CancellationToken cancellation) =>
            OpenAsync(host, port, token, true, cancellation);

        public static Task<SlgTcpClient> ConnectLoopbackForDevelopmentAsync(string host, int port, string token,
            CancellationToken cancellation)
        {
            if (!IPAddress.TryParse(host, out var address) || !IPAddress.IsLoopback(address))
                throw new ArgumentException("Plaintext requires a literal loopback address.", nameof(host));
            return OpenAsync(host, port, token, false, cancellation);
        }

        private static async Task<SlgTcpClient> OpenAsync(string host, int port, string token, bool tls,
            CancellationToken cancellation)
        {
            if (string.IsNullOrEmpty(token)) throw new ArgumentException("Login token required.", nameof(token));
            cancellation.ThrowIfCancellationRequested();
            var client = new SlgTcpClient();
            using (cancellation.Register(client.socket.Close))
            {
                try
                {
                    await client.socket.ConnectAsync(host, port).ConfigureAwait(false);
                    client.stream = client.socket.GetStream();
                    if (tls)
                    {
                        var secure = new SslStream(client.stream, false);
                        client.stream = secure;
                        await secure.AuthenticateAsClientAsync(host).ConfigureAwait(false);
                    }
                    // 认证与其他操作使用同一关联和解码路径。认证成功之前不把连接交给玩法调用方。
                    _ = client.ReceiveAsync();
                    var welcome = await client.AuthAsync(Guid.NewGuid().ToString("D"),
                        new GatewayAuth { Token = token }, cancellation).ConfigureAwait(false);
                    client.AcceptSession(welcome);
                    cancellation.ThrowIfCancellationRequested();
                    return client;
                }
                catch (GameplayException failure)
                {
                    client.Dispose();
                    throw new GatewayConnectionException(new BusinessError { Code = failure.Code, Message = failure.Message });
                }
                catch
                {
                    client.Dispose();
                    cancellation.ThrowIfCancellationRequested();
                    throw;
                }
            }
        }

        private void AcceptSession(GatewayAuthOk welcome)
        {
            // 不能将缺字段的旧 AUTH_OK 当成已完成 GAME 绑定；即便 TCP 响应 status=OK，
            // 不完整的会话身份也必须关连接，而不是允许业务在没有 fencing 的情况下开始。
            if (!long.TryParse(welcome.AccountId, NumberStyles.None, CultureInfo.InvariantCulture, out long account)
                || account <= 0 || account.ToString(CultureInfo.InvariantCulture) != welcome.AccountId
                || welcome.ExpiresAt <= DateTimeOffset.UtcNow.ToUnixTimeSeconds()
                || !Guid.TryParseExact(welcome.GameInstance, "D", out var instance) || instance == Guid.Empty
                || instance.ToString("D") != welcome.GameInstance || welcome.SessionGeneration <= 0)
                throw new InvalidDataException("Gateway authentication omitted a valid GAME session binding.");
            lock (state)
            {
                if (closeReason != null) throw closeReason;
                AccountId = welcome.AccountId;
                ExpiresAt = welcome.ExpiresAt;
                GameInstance = welcome.GameInstance;
                SessionGeneration = welcome.SessionGeneration;
            }
        }

        // 只有生成的 API 调用此入口。公开调用方法的请求/响应类型、编号和执行域均来自同一份 proto。
        private async Task<T> SendTypedAsync<T>(string requestId, uint msgId, IMessage request,
            MessageParser<T> parser, CancellationToken cancellation) where T : IMessage<T>
        {
            cancellation.ThrowIfCancellationRequested();
            WireCodec.RequireRequestId(requestId);
            if (request == null) throw new ArgumentNullException(nameof(request));
            if (!WireRoutes.ClientMayRequest(msgId) || WireRoutes.RequestIdFor(request) != msgId)
                throw new ArgumentException("The message is not registered as a client request.", nameof(request));
            if (request.CalculateSize() > WireCodec.MaxClientLength - WireCodec.RemainderHeaderBytes)
                throw new ArgumentException("Command too large.", nameof(request));
            Lane lane = WireRoutes.Domain(msgId) == WireDomain.Game ? Lane.Game
                : WireRoutes.Domain(msgId) == WireDomain.Aoi
                    ? (msgId == WireRoutes.MAP_QUERY ? Lane.MapQuery : Lane.MapControl) : Lane.Gateway;
            var call = new PendingCall(msgId, lane);
            lock (state)
            {
                if (closeReason != null) throw closeReason;
                if (pending.ContainsKey(requestId)) throw new InvalidOperationException("This requestId is already in flight.");
                foreach (var existing in pending.Values)
                    if (existing.Lane == lane) throw new InvalidOperationException("Await the pending request in this lane first.");
                pending.Add(requestId, call);
            }
            try
            {
                // 地图查询取消只结束等待，仍保留名额直至迟到回包被唯一接收循环回收；其它请求取消会断开，
                // 让未确认写操作保留明确的未知结果边界，避免把迟到回包当成下一次操作成功。
                using (cancellation.Register(() => {
                    if (lane == Lane.MapQuery)
                    {
                        lock (state) call.Completion.TrySetCanceled();
                    }
                    else Dispose();
                }))
                {
                    try { await WriteAsync(new WireFrame(WireFrame.Request, msgId, requestId, 0, request.ToByteString())).ConfigureAwait(false); }
                    catch (Exception failure) { Close(failure); }
                    WireFrame response;
                    try { response = await call.Completion.Task.ConfigureAwait(false); }
                    catch { cancellation.ThrowIfCancellationRequested(); throw; }
                    // 到这里已经收到并匹配了确定回包。即使调用方此刻取消，也不能把已确认的业务拒绝
                    // 降级成 OperationCanceledException；取消只影响等待，不能改写已经收到的业务结果。
                    if (response.Status != 0) throw new GameplayException(response, ReadError(response));
                    try { return parser.ParseFrom(response.Body); }
                    catch (InvalidProtocolBufferException invalid)
                    {
                        var failure = new InvalidDataException("Gateway sent an invalid typed response.", invalid);
                        Close(failure); throw failure;
                    }
                }
            }
            finally
            {
                lock (state)
                {
                    if (pending.TryGetValue(requestId, out var current) && current == call && !call.Completion.Task.IsCanceled)
                        pending.Remove(requestId);
                }
            }
        }

        private async Task ReceiveAsync()
        {
            try
            {
                while (true)
                {
                    var frame = await ReadAsync().ConfigureAwait(false);
                    if (frame.Kind == WireFrame.Request)
                    {
                        if (!WireRoutes.ServerMayRequest(frame.MsgId)) throw new InvalidDataException("Unexpected server request direction.");
                        var message = WireRoutes.ParseRequest(frame.MsgId, frame.Body);
                        if (!(message is GatewayHeartbeatRequest ping)) throw new InvalidDataException("Unsupported server request.");
                        var pong = new GatewayHeartbeatReply { Nonce = ping.Nonce };
                        await WriteAsync(new WireFrame(WireFrame.Response, WireRoutes.ResponseIdFor(pong),
                            frame.RequestId, 0, pong.ToByteString())).ConfigureAwait(false);
                    }
                    else if (frame.Kind == WireFrame.Push)
                    {
                        // 类型分发从同一份注册表生成；新增推送只需新增协议及对应业务接收器，外层帧不再增加字段。
                        if (!WireRoutes.IsPush(frame.MsgId)) throw new InvalidDataException("Unregistered push msgId.");
                        WireRoutes.DispatchPush(frame.MsgId, frame.Body, this);
                    }
                    else
                    {
                        if (!WireRoutes.ClientMayRequest(frame.MsgId)) throw new InvalidDataException("Unexpected response direction.");
                        if (frame.Status != 0) ReadError(frame);
                        lock (state)
                        {
                            if (!pending.TryGetValue(frame.RequestId, out var call) || call.MsgId != frame.MsgId)
                                throw new InvalidDataException("Unexpected gateway response identity.");
                            if (call.Completion.Task.IsCanceled) pending.Remove(frame.RequestId);
                            else if (!call.Completion.TrySetResult(frame)) throw new InvalidDataException("Duplicate gateway response.");
                        }
                    }
                }
            }
            catch (Exception failure) { Close(failure); }
        }

        void WireRoutes.IPushHandlers.OnGatewayConnectionError(GatewayConnectionError notification)
        {
            if (notification.Error == null || string.IsNullOrWhiteSpace(notification.Error.Code))
                throw new InvalidDataException("Missing connection error details.");
            throw new GatewayConnectionException(notification.Error);
        }

        private static BusinessError ReadError(WireFrame frame)
        {
            var error = BusinessError.Parser.ParseFrom(frame.Body);
            if (string.IsNullOrWhiteSpace(error.Code)) throw new InvalidDataException("A failure response requires a business error code.");
            return error;
        }

        private async Task WriteAsync(WireFrame frame)
        {
            // 一次写入口提交完整逻辑帧，不能让两个业务线程分别写头/正文，导致中间插入另一条消息。
            byte[] bytes = WireCodec.Encode(frame, WireCodec.MaxClientLength);
            await writer.WaitAsync().ConfigureAwait(false);
            try { await stream.WriteAsync(bytes, 0, bytes.Length).ConfigureAwait(false); }
            finally { writer.Release(); }
        }

        private async Task<WireFrame> ReadAsync()
        {
            var lengthBytes = new byte[4];
            await ReadFullyAsync(lengthBytes).ConfigureAwait(false);
            int length = WireCodec.ReadLength(lengthBytes, WireCodec.MaxServerLength);
            var remainder = new byte[length];
            await ReadFullyAsync(remainder).ConfigureAwait(false);
            return WireCodec.Decode(remainder, WireCodec.MaxServerLength);
        }

        private async Task ReadFullyAsync(byte[] bytes)
        {
            int offset = 0;
            while (offset < bytes.Length)
            {
                int count = await stream.ReadAsync(bytes, offset, bytes.Length - offset).ConfigureAwait(false);
                if (count == 0) throw new EndOfStreamException("Gateway disconnected.");
                offset += count;
            }
        }

        private void Close(Exception reason)
        {
            lock (state)
            {
                if (closeReason != null) return;
                closeReason = reason;
                foreach (var call in pending.Values) call.Completion.TrySetException(reason);
                pending.Clear();
                mapSubscription?.Fail(reason);
            }
            socket.Close(); stream?.Dispose();
        }
        public void Dispose() => Close(new ObjectDisposedException(nameof(SlgTcpClient)));
    }
}
