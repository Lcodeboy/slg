package com.slg.gate;

import com.slg.rpc.GatewayWire;
import com.slg.rpc.WireFrame;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.MessageToMessageDecoder;
import java.util.List;

/**
 * 固定消息头与 Netty 的唯一适配点。LengthFieldBasedFrameDecoder 已取出完整帧并去掉四字节长度，
 * 解码器只消费剩余头和正文；TCP 分段或粘包不会改变业务消息边界。输入 ByteBuf 由 Netty 自动释放，
 * WireFrame 持有独立的不可变正文，异步 gRPC 不会访问已归还内存池的输入缓冲。
 */
final class TcpFrameCodec {
    private TcpFrameCodec() { }

    /** 四字节长度一到达就检查最小头长，不能让一个永远不可能合法的半包占到读超时。 */
    static final class Framer extends LengthFieldBasedFrameDecoder {
        Framer() { super(GatewayWire.MAX_CLIENT_FRAME_BYTES + 4, 0, 4, 0, 4); }
        @Override protected Object decode(ChannelHandlerContext ctx, ByteBuf input) throws Exception {
            if (input.readableBytes() >= 4 && input.getUnsignedInt(input.readerIndex()) < GatewayWire.REMAINDER_HEADER_BYTES) {
                // 该连接即将断开，不尝试从无效边界恢复。消费已有字节，避免关闭时 decodeLast 重报同一错误。
                input.skipBytes(input.readableBytes());
                throw new CorruptedFrameException("Length is shorter than the fixed header");
            }
            return super.decode(ctx, input);
        }
    }

    static final class Decoder extends MessageToMessageDecoder<ByteBuf> {
        @Override protected void decode(ChannelHandlerContext ctx, ByteBuf frame, List<Object> output) {
            output.add(GatewayWire.decode(frame.nioBuffer()));
        }
    }

    /**
     * 一次逻辑 write 生成包含长度、消息头、正文的一个输出 ByteBuf。业务线程不能分两次 write 头/正文，
     * 避免并发发送时被其他消息插入；flush 仍由连接处理器按业务背压控制。
     */
    static final class Encoder extends MessageToByteEncoder<WireFrame> {
        @Override protected void encode(ChannelHandlerContext ctx, WireFrame frame, ByteBuf output) {
            int length = GatewayWire.encodedLength(frame);
            output.ensureWritable(length);
            int start = output.writerIndex();
            // 直接写入池化 ByteBuf 的 NIO 视图，不再先分配整包 byte[] 又复制一次。
            // 具体字段顺序与 UUID 字节序只在 GatewayWire 定义，适配层不能另写第二套协议。
            GatewayWire.encode(frame, output.nioBuffer(start, length));
            output.writerIndex(start + length);
        }
    }
}
