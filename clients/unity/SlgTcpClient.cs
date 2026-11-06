using System;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Text.RegularExpressions;
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

    /// <summary>网关或 GAME 对指定请求的错误回包。Code 为 RESULT_UNKNOWN 等未知结果时，重试沿用原请求 ID。</summary>
    public sealed class GameplayException : Exception
    {
        public string RequestId { get; }
        public string Code { get; }
        public int Status { get; }

        internal GameplayException(PlayReply reply) : base(reply.Error.Message)
        {
            RequestId = reply.RequestId;
            Code = reply.Error.Code;
            Status = reply.Status;
        }
    }

    /// <summary>一条 Unity 原生客户端连接。连接级错误或结果未知时，由调用方决定重登和查回执。</summary>
    public sealed partial class SlgTcpClient : IDisposable
    {
        private const uint Version = 2;
        private const int MaxOutbound = 65536, MaxInbound = 1048592;
        private readonly TcpClient socket = new TcpClient { NoDelay = true };
        private readonly SemaphoreSlim writer = new SemaphoreSlim(1, 1);
        private readonly object state = new object();
        private Stream stream;
        private Exception closeReason;
        private string pendingId;
        // 匹配操作编号与请求 ID，避免将另一条写操作的回包误判为成功。
        private uint pendingMsgId;
        private TaskCompletionSource<PlayReply> pending;

        public string AccountId { get; private set; }
        public long ExpiresAt { get; private set; } // Unix 秒。

        private SlgTcpClient() {}

        /// <summary>校验服务端证书链和 host，再发送登录令牌。由调用方提供连接/鉴权截止时间。</summary>
        public static Task<SlgTcpClient> ConnectAsync(string host, int port, string token, CancellationToken cancellation)
        {
            return OpenAsync(host, port, token, true, cancellation);
        }

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
            // 较旧的 Unity 运行库没有带 CancellationToken 的 ConnectAsync/TLS 握手。
            // 直接关闭底层 socket 也能中止尚未完成的握手和半包读取。
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
                    await client.WriteAsync(new GatewayFrame { Version = Version, Auth = new GatewayAuth { Token = token } })
                        .ConfigureAwait(false);
                    var welcome = await client.ReadAsync().ConfigureAwait(false);
                    if (welcome.AuthOk == null) throw new InvalidDataException("Expected AUTH_OK.");
                    client.AccountId = welcome.AuthOk.AccountId;
                    client.ExpiresAt = welcome.AuthOk.ExpiresAt;
                    cancellation.ThrowIfCancellationRequested();
                    _ = client.ReceiveAsync();
                    return client;
                }
                catch
                {
                    client.Dispose();
                    cancellation.ThrowIfCancellationRequested();
                    throw;
                }
            }
        }

        // 由生成的玩法方法提供唯一的编号和响应解析器，调用方无需自行填 msgId 或解包。
        // 先限制消息体，再序列化，避免超大输入在写入 socket 前就分配无界缓冲。
        private async Task<T> SendTypedAsync<T>(string requestId, uint msgId, IMessage request,
            MessageParser<T> parser, CancellationToken cancellation) where T : IMessage<T>
        {
            cancellation.ThrowIfCancellationRequested();
            if (string.IsNullOrEmpty(requestId) || !Regex.IsMatch(requestId, @"\A[A-Za-z0-9_-]{1,64}\z"))
                throw new ArgumentException("Invalid request_id.", nameof(requestId));
            if (request == null) throw new ArgumentNullException(nameof(request));
            if (request.CalculateSize() > MaxOutbound) throw new ArgumentException("Command too large.", nameof(request));
            var frame = new GatewayFrame {
                Version = Version, PlayRequest = new PlayRequest {
                    MsgId = msgId, RequestId = requestId, Body = request.ToByteString()
                }
            };
            if (frame.CalculateSize() > MaxOutbound) throw new ArgumentException("Command too large.", nameof(frame));
            var reply = new TaskCompletionSource<PlayReply>(TaskCreationOptions.RunContinuationsAsynchronously);
            lock (state)
            {
                if (closeReason != null) throw closeReason;
                if (pending != null) throw new InvalidOperationException("Await the pending command first.");
                pending = reply;
                pendingId = requestId;
                pendingMsgId = msgId;
            }
            try
            {
                using (cancellation.Register(Dispose))
                {
                    try { await WriteAsync(frame).ConfigureAwait(false); }
                    catch (Exception e) { Close(e); }
                    // 所有 I/O 失败从同一个 Task 返回。异常结果不会被下一条命令误领。
                    try
                    {
                        PlayReply response = await reply.Task.ConfigureAwait(false);
                        if (response.OutcomeCase == PlayReply.OutcomeOneofCase.Error) throw new GameplayException(response);
                        return parser.ParseFrom(response.Body);
                    }
                    catch (InvalidProtocolBufferException invalid)
                    {
                        // 正文损坏时不放行下一请求，先断开这条已无法确认结果的连接。
                        var failure = new InvalidDataException("Gateway sent an invalid response.", invalid);
                        Close(failure);
                        throw failure;
                    }
                    catch { cancellation.ThrowIfCancellationRequested(); throw; }
                }
            }
            finally { lock (state) { pending = null; pendingId = null; pendingMsgId = 0; } }
        }

        // 只有这个循环读取已认证连接，玩家不操作时也能处理 PING。心跳写入与业务写入共用写锁。
        private async Task ReceiveAsync()
        {
            try
            {
                while (true)
                {
                    var frame = await ReadAsync().ConfigureAwait(false);
                    if (frame.Ping != null)
                    {
                        await WriteAsync(new GatewayFrame { Version = Version, Pong = frame.Ping }).ConfigureAwait(false);
                        continue;
                    }
                    lock (state)
                    {
                        if (pending == null || !MatchesPending(frame.PlayReply) || !pending.TrySetResult(frame.PlayReply))
                            throw new InvalidDataException("Unexpected gateway reply.");
                    }
                }
            }
            catch (Exception e) { Close(e); }
        }

        // 单请求在途仍需校验关联字段，避免重连、协议版本不一致或服务端误投时把别的回包当做出征成功。
        // 显式 oneof body 允许成功消息序列化为空；同样的空字节在 outcome 缺失时应当断连。
        private bool MatchesPending(PlayReply reply)
        {
            if (reply == null || reply.MsgId != pendingMsgId || reply.RequestId != pendingId) return false;
            if (reply.OutcomeCase == PlayReply.OutcomeOneofCase.Body)
                return reply.Status == 200;
            // GAME 可以用 200 携带确定的业务拒绝。分支由 outcome 决定，保留 status 和原错误码。
            return reply.OutcomeCase == PlayReply.OutcomeOneofCase.Error && !string.IsNullOrWhiteSpace(reply.Error.Code)
                && reply.Status >= 200 && reply.Status <= 599;
        }

        private async Task WriteAsync(GatewayFrame frame)
        {
            var length = frame.CalculateSize();
            if (length <= 0 || length > MaxOutbound) throw new InvalidDataException("Invalid outgoing frame size.");
            var bytes = new byte[4 + length];
            bytes[0] = (byte)(length >> 24); bytes[1] = (byte)(length >> 16);
            bytes[2] = (byte)(length >> 8);  bytes[3] = (byte)length;
            using (var buffer = new MemoryStream(bytes, 4, length, true)) frame.WriteTo(buffer);
            await writer.WaitAsync().ConfigureAwait(false);
            try { await stream.WriteAsync(bytes, 0, bytes.Length).ConfigureAwait(false); }
            finally { writer.Release(); }
        }

        private async Task<GatewayFrame> ReadAsync()
        {
            var header = new byte[4];
            await ReadFullyAsync(header).ConfigureAwait(false);
            uint length = ((uint)header[0] << 24) | ((uint)header[1] << 16) | ((uint)header[2] << 8) | header[3];
            if (length == 0 || length > MaxInbound) throw new InvalidDataException("Invalid incoming frame size.");
            var body = new byte[(int)length];
            await ReadFullyAsync(body).ConfigureAwait(false);
            var frame = GatewayFrame.Parser.ParseFrom(body);
            if (frame.Version != Version) throw new InvalidDataException("Unsupported gateway frame version.");
            if (frame.Error != null) throw new GatewayConnectionException(frame.Error);
            return frame;
        }

        private async Task ReadFullyAsync(byte[] bytes)
        {
            int offset = 0;
            while (offset < bytes.Length)
            {
                int n = await stream.ReadAsync(bytes, offset, bytes.Length - offset).ConfigureAwait(false);
                if (n == 0) throw new EndOfStreamException("Gateway disconnected.");
                offset += n;
            }
        }

        private void Close(Exception reason)
        {
            lock (state)
            {
                if (closeReason != null) return;
                closeReason = reason;
                pending?.TrySetException(reason);
            }
            socket.Close();
            stream?.Dispose();
        }

        public void Dispose() => Close(new ObjectDisposedException(nameof(SlgTcpClient)));
    }
}
