package com.slg.gate;

import com.slg.rpc.GatewayWire;
import com.slg.rpc.WireFrame;
import com.slg.rpc.generated.WireRoutes;
import com.slg.rpc.v1.GatewayHeartbeatRequest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** 覆盖真实 Netty 分帧/内存所有权边界，而不仅验证同一个序列化函数的自我往返。 */
class TcpFrameCodecTest {
    @Test void everyHeaderSplitAndCoalescedFramesDeliverExactlyOneIndependentMessageEach() {
        var first = WireFrame.request(WireRoutes.HEARTBEAT, UUID.fromString("01234567-89ab-cdef-0123-456789abcdef"),
                GatewayHeartbeatRequest.newBuilder().setNonce(123456789).build());
        var second = WireFrame.request(WireRoutes.HEARTBEAT, UUID.fromString("fedcba98-7654-3210-fedc-ba9876543210"),
                GatewayHeartbeatRequest.getDefaultInstance());
        byte[] one = GatewayWire.encode(first), two = GatewayWire.encode(second);
        // 每一个头部切点都模拟一次真实 TCP 半包；第二次 read 又同时包含完整的下一帧。
        for (int split = 1; split < GatewayWire.HEADER_BYTES; split++) {
            var channel = new EmbeddedChannel(new TcpFrameCodec.Framer(), new TcpFrameCodec.Decoder());
            try {
                assertFalse(channel.writeInbound(Unpooled.wrappedBuffer(one, 0, split)));
                assertNull(channel.readInbound());
                var rest = Unpooled.buffer(one.length - split + two.length)
                        .writeBytes(one, split, one.length - split).writeBytes(two);
                assertTrue(channel.writeInbound(rest));
                WireFrame received = channel.readInbound();
                assertEquals(first, received);
                assertEquals(second, channel.readInbound());
                assertNull(channel.readInbound());
                assertEquals(0, rest.refCnt(), "Netty输入缓冲必须被释放，异步消息不能依赖其生命周期");
                assertEquals(first.body(), received.body(), "释放输入后正文仍有效");
            } finally { channel.finishAndReleaseAll(); }
        }
    }

    @Test void oneLogicalWriteProducesOneCompleteFrameIncludingAnEmptyProtobufBody() {
        var frame = WireFrame.request(WireRoutes.HEARTBEAT, UUID.fromString("01234567-89ab-cdef-0123-456789abcdef"),
                GatewayHeartbeatRequest.getDefaultInstance());
        var channel = new EmbeddedChannel(new TcpFrameCodec.Encoder());
        try {
            assertTrue(channel.writeOutbound(frame));
            ByteBuf bytes = channel.readOutbound();
            try {
                assertEquals(GatewayWire.HEADER_BYTES, bytes.readableBytes());
                assertEquals(GatewayWire.REMAINDER_HEADER_BYTES, bytes.readUnsignedInt());
                assertEquals(frame, GatewayWire.decode(bytes.nioBuffer()));
            } finally { bytes.release(); }
            assertNull(channel.readOutbound(), "头和正文不能成为可被其他消息插入的独立输出项");
        } finally { channel.finishAndReleaseAll(); }
    }

    @Test void impossibleLengthsAreRejectedAsSoonAsTheFourByteLengthArrives() {
        for (int length : new int[]{0, 25, GatewayWire.MAX_CLIENT_FRAME_BYTES + 1, -1}) {
            var channel = new EmbeddedChannel(new TcpFrameCodec.Framer(), new TcpFrameCodec.Decoder());
            try {
                assertThrows(DecoderException.class, () -> channel.writeInbound(Unpooled.buffer(4).writeInt(length)));
                assertNull(channel.readInbound());
            } finally { channel.finishAndReleaseAll(); }
        }
    }
}
