package com.slg.rpc;

import com.google.protobuf.ByteString;
import com.slg.rpc.generated.GameplayRoutes;
import com.slg.rpc.v1.GatewayAuth;
import com.slg.rpc.v1.GatewayFrame;
import com.slg.rpc.v1.GatewayHeartbeat;
import com.slg.rpc.v1.PlayReply;
import com.slg.rpc.v1.PlayRequest;
import com.slg.rpc.v1.StartMarchReq;
import com.slg.rpc.v1.TroopGroup;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.*;

class GatewayWireTest {
    // 固定两条最小帧，供其他语言的客户端在没有服务端时检查字段号、长度语义和字节序。
    @Test void referenceAuthAndPingFramesAreStable() {
        var auth = GatewayFrame.newBuilder().setVersion(GatewayWire.VERSION)
                .setAuth(GatewayAuth.newBuilder().setToken("abc")).build();
        var ping = GatewayFrame.newBuilder().setVersion(GatewayWire.VERSION)
                .setPing(GatewayHeartbeat.newBuilder().setNonce(1)).build();
        assertEquals("00000009080212050a03616263", frameHex(auth));
        assertEquals("00000006080232020801", frameHex(ping));
    }

    @Test void commandKeepsOriginalReceiptIdAndTypedPayload() throws Exception {
        var order = StartMarchReq.newBuilder().addTroops(TroopGroup.newBuilder().setCount(9_007_199_254_740_993L)).build();
        var request = PlayRequest.newBuilder().setMsgId(GameplayRoutes.START_MARCH)
                .setRequestId("retry-19").setBody(order.toByteString()).build();
        var bytes = GatewayFrame.newBuilder().setVersion(GatewayWire.VERSION).setPlayRequest(request).build().toByteArray();
        var wire = GatewayFrame.parseFrom(bytes);
        assertEquals(request, wire.getPlayRequest());
        assertEquals(order, StartMarchReq.parseFrom(wire.getPlayRequest().getBody()));
    }

    @Test void maximumGameReplyStillFitsInsideServerFrame() {
        // body 的标签占一个字节，此长度区间的 varint 占三个字节。
        var result = PlayReply.newBuilder().setBody(ByteString.copyFrom(new byte[RpcTransport.MAX_RESPONSE_BYTES - 4])).build();
        assertEquals(RpcTransport.MAX_RESPONSE_BYTES, result.getSerializedSize());
        var frame = GatewayFrame.newBuilder().setVersion(GatewayWire.VERSION).setPlayReply(result).build();
        assertTrue(frame.getSerializedSize() <= GatewayWire.MAX_SERVER_FRAME_BYTES);
    }

    private static String frameHex(GatewayFrame frame) {
        var bytes = frame.toByteArray();
        return HexFormat.of().formatHex(ByteBuffer.allocate(4 + bytes.length).putInt(bytes.length).put(bytes).array());
    }
}
