using System;
using System.IO;
using Google.Protobuf;
using Slg.Rpc.V1;

namespace Slg.Unity
{
    /// <summary>
    /// TCP 传输帧，不含任何具体玩法字段。请求/响应用同一个稳定 UUID 关联；推送没有请求，UUID 固定为零。
    /// msgId 与 Protobuf 类型的映射由协议生成，不能在本文件另建一张手写编号表。
    /// </summary>
    internal sealed class WireFrame
    {
        internal const byte Request = 1, Response = 2, Push = 3;
        internal readonly byte Kind;
        internal readonly uint MsgId;
        internal readonly string RequestId;
        internal readonly uint Status;
        internal readonly ByteString Body;

        internal WireFrame(byte kind, uint msgId, string requestId, uint status, ByteString body)
        {
            Kind = kind; MsgId = msgId; RequestId = requestId; Status = status;
            Body = body ?? throw new ArgumentNullException(nameof(body));
        }
    }

    /// <summary>
    /// v3: length[4] + version[1] + kind[1] + msgId[4] + requestId[16] + status[4] + protobuf。
    /// length 不含自身，计算剩余 26 字节头和正文；整数采用大端。正文可以为空，例如没有字段的查询。
    /// 长度先验证后分配，4 字节长度字段绝不代表允许接收 4 GiB 数据。
    /// </summary>
    internal static class WireCodec
    {
        internal const byte Version = 3;
        internal const int HeaderBytes = 30, RemainderHeaderBytes = 26;
        internal const int MaxClientLength = 65536 + RemainderHeaderBytes;
        internal const int MaxServerLength = 1048576 + RemainderHeaderBytes;
        internal const string ZeroRequestId = "00000000-0000-0000-0000-000000000000";
        private const string Hex = "0123456789abcdef";

        internal static string RequireRequestId(string value)
        {
            // 规范形式既是传输 UUID 的唯一文本表示，也是 GAME 原卷回执的键。大小写/缩写别名不进入协议，
            // 避免同一个二进制 UUID 在重连时映射到两个字符串键。新操作分配新 ID，同一次重试必须沿用原 ID。
            if (!Guid.TryParseExact(value, "D", out var id) || id == Guid.Empty || value != id.ToString("D"))
                throw new ArgumentException("requestId must be a nonzero lowercase canonical UUID.", nameof(value));
            return value;
        }

        internal static byte[] Encode(WireFrame frame, int maximumLength)
        {
            Validate(frame);
            if (frame.Body.Length > maximumLength - RemainderHeaderBytes)
                throw new InvalidDataException("Outgoing frame exceeds the payload budget.");
            int length = RemainderHeaderBytes + frame.Body.Length;
            var bytes = new byte[4 + length];
            Write32(bytes, 0, (uint)length);
            bytes[4] = Version; bytes[5] = frame.Kind;
            Write32(bytes, 6, frame.MsgId);
            // Guid.ToByteArray() 使用 .NET 混合端序，不能直接放在线路上。按规范 UUID 的十六进制顺序写入，
            // 对应 Java UUID 的 mostSignificantBits / leastSignificantBits 各自大端编码。
            int position = 10;
            string digits = frame.RequestId.Replace("-", "");
            for (int i = 0; i < digits.Length; i += 2)
                bytes[position++] = (byte)((Hex.IndexOf(digits[i]) << 4) | Hex.IndexOf(digits[i + 1]));
            Write32(bytes, 26, frame.Status);
            frame.Body.CopyTo(bytes, HeaderBytes);
            return bytes;
        }

        internal static int ReadLength(byte[] bytes, int maximumLength)
        {
            if (bytes == null || bytes.Length != 4) throw new InvalidDataException("Truncated length field.");
            uint length = Read32(bytes, 0);
            if (length < RemainderHeaderBytes || length > maximumLength)
                throw new InvalidDataException("Invalid incoming frame length.");
            return (int)length;
        }

        // 输入为已按 length 完整读取的剩余帧；长度头由唯一读取循环消费，TCP 半包不会被当成业务消息。
        internal static WireFrame Decode(byte[] remainder, int maximumLength)
        {
            if (remainder == null || remainder.Length < RemainderHeaderBytes || remainder.Length > maximumLength)
                throw new InvalidDataException("Invalid incoming frame size.");
            if (remainder[0] != Version) throw new InvalidDataException("Unsupported gateway wire version.");
            var characters = new char[36];
            int position = 0;
            for (int i = 6; i < 22; i++)
            {
                if (position == 8 || position == 13 || position == 18 || position == 23) characters[position++] = '-';
                characters[position++] = Hex[remainder[i] >> 4];
                characters[position++] = Hex[remainder[i] & 15];
            }
            var frame = new WireFrame(remainder[1], Read32(remainder, 2), new string(characters), Read32(remainder, 22),
                ByteString.CopyFrom(remainder, RemainderHeaderBytes, remainder.Length - RemainderHeaderBytes));
            Validate(frame);
            return frame;
        }

        private static void Validate(WireFrame frame)
        {
            if (frame.Status > int.MaxValue || !Enum.IsDefined(typeof(WireResultStatus), (int)frame.Status))
                throw new InvalidDataException("Unsupported application result status.");
            if (frame.MsgId == 0 || frame.MsgId > int.MaxValue)
                throw new InvalidDataException("msgId is outside the registered protocol range.");
            if (frame.Kind != WireFrame.Request && frame.Kind != WireFrame.Response && frame.Kind != WireFrame.Push)
                throw new InvalidDataException("Unsupported frame kind.");
            if (frame.Kind == WireFrame.Push)
            {
                if (frame.RequestId != ZeroRequestId || frame.Status != 0)
                    throw new InvalidDataException("A push must have a zero requestId and status.");
            }
            else
            {
                try { RequireRequestId(frame.RequestId); }
                catch (ArgumentException failure) { throw new InvalidDataException("Invalid wire requestId.", failure); }
                if (frame.Kind == WireFrame.Request && frame.Status != 0)
                    throw new InvalidDataException("A request cannot carry a response status.");
            }
            if (frame.Kind == WireFrame.Response && frame.Status != 0)
            {
                try
                {
                    if (string.IsNullOrWhiteSpace(BusinessError.Parser.ParseFrom(frame.Body).Code))
                        throw new InvalidDataException("A failure response requires a business error code.");
                }
                catch (InvalidProtocolBufferException failure)
                { throw new InvalidDataException("Invalid failure response body.", failure); }
            }
        }

        private static uint Read32(byte[] bytes, int offset) =>
            ((uint)bytes[offset] << 24) | ((uint)bytes[offset + 1] << 16) | ((uint)bytes[offset + 2] << 8) | bytes[offset + 3];
        private static void Write32(byte[] bytes, int offset, uint value)
        {
            bytes[offset] = (byte)(value >> 24); bytes[offset + 1] = (byte)(value >> 16);
            bytes[offset + 2] = (byte)(value >> 8); bytes[offset + 3] = (byte)value;
        }
    }
}
