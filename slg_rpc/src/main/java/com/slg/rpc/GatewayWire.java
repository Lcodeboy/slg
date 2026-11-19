package com.slg.rpc;

import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.BufferOverflowException;
import java.util.Objects;
import java.util.UUID;

/**
 * Unity TCP v3：length(4) | version(1) | kind(1) | msgId(4) | requestId(16) | status(4) | body。
 * 全部整数为大端；length 只表示其后剩余字节，包括 26 字节公共头和独立 protobuf 正文。
 * 业务正文可以为空，合法帧的最小 length 是 26，不是 1，也不是 0。
 *
 * length 是无符号 32 位，但不代表允许分配 4 GiB。读端必须先用 validateLength 检查方向预算，
 * 再分配/等待正文；网关 Netty decoder 与 Unity 读取循环也执行同一上限，防止超长声明耗尽内存。
 */
public final class GatewayWire {
    public static final int VERSION = 3;
    public static final int HEADER_BYTES = 30;
    public static final int REMAINDER_HEADER_BYTES = HEADER_BYTES - Integer.BYTES;
    public static final int MAX_CLIENT_FRAME_BYTES = RpcTransport.MAX_REQUEST_BYTES + REMAINDER_HEADER_BYTES;
    public static final int MAX_SERVER_FRAME_BYTES = RpcTransport.MAX_RESPONSE_BYTES + REMAINDER_HEADER_BYTES;

    private GatewayWire() {}

    /** 头中的 128 位 ID 原样对应原卷回执的规范字符串；禁止截断、hash 或连接重置改写请求身份。 */
    public static UUID requestId(String text) {
        if (text == null || text.length() != 36) throw new IllegalArgumentException("Canonical UUID requestId required");
        UUID value = UUID.fromString(text);
        if (!value.toString().equals(text) || value.equals(WireFrame.NO_REQUEST))
            throw new IllegalArgumentException("Nonzero lowercase canonical UUID requestId required");
        return value;
    }

    public static String requestId(UUID value) {
        Objects.requireNonNull(value, "requestId");
        if (value.equals(WireFrame.NO_REQUEST)) throw new IllegalArgumentException("Nonzero requestId required");
        return value.toString();
    }

    public static int validateLength(long length, int maximum) {
        if (length < REMAINDER_HEADER_BYTES || length > maximum)
            throw new IllegalArgumentException("Invalid frame length");
        return (int) length;
    }

    /** 输出完整逻辑帧，包括长度；发送端交给 channel 一次 write，flush 由连接统一安排。 */
    public static byte[] encode(WireFrame frame) {
        ByteBuffer output = ByteBuffer.allocate(encodedLength(frame));
        encode(frame, output);
        return output.array();
    }

    /** 含 4 字节长度字段的完整包长；用于 Netty 池内缓冲区一次预留，不再分配中转 byte[]。 */
    public static int encodedLength(WireFrame frame) {
        int maximum = frame.kind() == WireFrame.REQUEST ? MAX_CLIENT_FRAME_BYTES : MAX_SERVER_FRAME_BYTES;
        int length = validateLength((long) REMAINDER_HEADER_BYTES + frame.body().size(), maximum);
        return Integer.BYTES + length;
    }

    /**
     * 在调用方提供的堆内/直接缓冲区中写入；成功后推进 position，不改变调用方原来的 byte order。
     * 空间不足时在写任何字节前失败。Netty 直接提供池内 ByteBuf 的 NIO 视图即可复用唯一布局。
     */
    public static void encode(WireFrame frame, ByteBuffer destination) {
        int total = encodedLength(frame);
        if (destination.remaining() < total) throw new BufferOverflowException();
        ByteBuffer output = destination.slice().order(ByteOrder.BIG_ENDIAN);
        output.putInt(total - Integer.BYTES).put((byte) VERSION).put((byte) frame.kind()).putInt(frame.msgId());
        // UUID 按文本的前 16 个十六进制数字、后 16 个数字分别写入，不使用 .NET Guid 的混合端序数组。
        output.putLong(frame.requestId().getMostSignificantBits()).putLong(frame.requestId().getLeastSignificantBits());
        output.putInt(frame.status()).put(frame.body().asReadOnlyByteBuffer());
        destination.position(destination.position() + total);
    }

    /** 输入已由长度解码器截取的一帧，不含最前面的 4 字节 length；不修改调用者 buffer 的位置。 */
    public static WireFrame decode(ByteBuffer remainder) {
        ByteBuffer input = remainder.slice().order(ByteOrder.BIG_ENDIAN);
        validateLength(input.remaining(), MAX_SERVER_FRAME_BYTES);
        if (Byte.toUnsignedInt(input.get()) != VERSION) throw new IllegalArgumentException("Unsupported wire version");
        int kind = Byte.toUnsignedInt(input.get()), msgId = input.getInt();
        UUID requestId = new UUID(input.getLong(), input.getLong());
        int status = input.getInt();
        return new WireFrame(kind, msgId, requestId, status, ByteString.copyFrom(input));
    }

    public static WireFrame decode(byte[] remainder) { return decode(ByteBuffer.wrap(remainder)); }
}
