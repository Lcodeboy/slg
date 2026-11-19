package com.slg.rpc;

import com.google.protobuf.ByteString;
import com.slg.rpc.generated.WireRoutes;
import com.slg.rpc.v1.BusinessError;
import com.slg.rpc.v1.GatewayAuth;
import com.slg.rpc.v1.GetPlayerReq;
import com.slg.rpc.v1.MapViewUpdate;
import com.slg.rpc.v1.PlayReply;
import com.slg.rpc.v1.StartMarchReq;
import com.slg.rpc.v1.TroopGroup;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.BufferOverflowException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class GatewayWireTest {
    private static final UUID ID = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");

    @Test void goldenVectorFixesLengthHeaderOffsetsAndUuidNetworkByteOrder() {
        // Python/Unity 共同黄金值：避免两端各自 roundtrip 正常、实际 UUID 混合端序却互不相通。
        var frame = new WireFrame(WireFrame.REQUEST, 0x01020304, ID, WireStatus.OK, ByteString.fromHex("0801"));
        byte[] bytes = GatewayWire.encode(frame);
        assertEquals("0000001c03010102030400112233445566778899aabbccddeeff000000000801", HexFormat.of().formatHex(bytes));
        assertEquals(frame, GatewayWire.decode(Arrays.copyOfRange(bytes, 4, bytes.length)));
        ByteBuffer prefixed = ByteBuffer.allocate(bytes.length + 10).put(bytes).flip();
        prefixed.position(4);
        assertEquals(frame, GatewayWire.decode(prefixed));
        assertEquals(4, prefixed.position(), "decode must not mutate its caller's buffer position");
    }

    @Test void businessIdAndOriginalProtobufPayloadSurviveTheWireWithoutAnOuterEnvelope() throws Exception {
        var order = StartMarchReq.newBuilder().addTroops(TroopGroup.newBuilder().setCount(9_007_199_254_740_993L)).build();
        var frame = WireFrame.request(WireRoutes.START_MARCH, ID, order);
        var decoded = roundtrip(frame);
        assertEquals(ID.toString(), GatewayWire.requestId(decoded.requestId()));
        assertEquals(order, WireRoutes.parseRequest(decoded.msgId(), decoded.body()));
        assertEquals(order.toByteString(), decoded.body());
    }

    @Test void pooledDirectBufferEncodingUsesTheSameLayoutWithoutChangingCallerByteOrder() {
        var frame = WireFrame.request(WireRoutes.AUTH, ID, GatewayAuth.newBuilder().setToken("abc").build());
        byte[] expected = GatewayWire.encode(frame);
        ByteBuffer direct = ByteBuffer.allocateDirect(expected.length + 7).order(ByteOrder.LITTLE_ENDIAN);
        direct.position(7);
        GatewayWire.encode(frame, direct);
        assertEquals(7 + expected.length, direct.position());
        assertEquals(ByteOrder.LITTLE_ENDIAN, direct.order());
        direct.flip().position(7);
        byte[] actual = new byte[expected.length]; direct.get(actual);
        assertArrayEquals(expected, actual);
        ByteBuffer small = ByteBuffer.allocate(expected.length - 1);
        assertThrows(BufferOverflowException.class, () -> GatewayWire.encode(frame, small));
        assertEquals(0, small.position());
        assertArrayEquals(new byte[small.capacity()], small.array(), "Failure must not leave a partially written header");
    }

    @Test void emptySuccessfulProtobufIsStillACompleteFrame() {
        byte[] bytes = GatewayWire.encode(WireFrame.request(WireRoutes.GET_PLAYER, ID, GetPlayerReq.getDefaultInstance()));
        assertEquals(30, bytes.length);
        assertEquals(26, ByteBuffer.wrap(bytes).getInt());
        assertTrue(GatewayWire.decode(Arrays.copyOfRange(bytes, 4, bytes.length)).body().isEmpty());
    }

    @Test void uuidStringIsCanonicalAndDoesNotChangeTheDurableReceiptIdentity() {
        assertEquals(ID, GatewayWire.requestId(ID.toString()));
        for (String invalid : new String[] { "retry-19", "1-2-3-4-5", ID.toString().toUpperCase(),
                ID.toString().replace("-", ""), "00000000-0000-0000-0000-000000000000" })
            assertThrows(IllegalArgumentException.class, () -> GatewayWire.requestId(invalid));
        assertThrows(IllegalArgumentException.class, () -> GatewayWire.requestId(WireFrame.NO_REQUEST));
    }

    @Test void pushUsesZeroRequestIdAndNoOutcomeStatus() {
        var update = MapViewUpdate.newBuilder().setSubscriptionId("view-1").setSnapshot(true).build();
        WireFrame frame = roundtrip(WireRoutes.push(update));
        assertEquals(WireFrame.PUSH, frame.kind());
        assertEquals(WireRoutes.MAP_VIEW_UPDATE, frame.msgId());
        assertEquals(WireFrame.NO_REQUEST, frame.requestId());
        assertEquals(WireStatus.OK, frame.status());
    }

    @Test void malformedHeadersAreRejectedBeforeBusinessDecoding() {
        byte[] remainder = Arrays.copyOfRange(GatewayWire.encode(WireFrame.request(WireRoutes.AUTH, ID,
                GatewayAuth.newBuilder().setToken("abc").build())), 4, 35);
        assertThrows(IllegalArgumentException.class, () -> GatewayWire.decode(new byte[25]));
        for (int[] mutation : new int[][] { {0, 2}, {1, 0}, {1, 4}, {2, 128}, {25, 1} }) {
            byte[] corrupt = remainder.clone(); corrupt[mutation[0]] = (byte) mutation[1];
            assertThrows(IllegalArgumentException.class, () -> GatewayWire.decode(corrupt));
        }
        byte[] zero = remainder.clone(); Arrays.fill(zero, 6, 22, (byte) 0);
        assertThrows(IllegalArgumentException.class, () -> GatewayWire.decode(zero));
        assertThrows(IllegalArgumentException.class,
                () -> new WireFrame(WireFrame.RESPONSE, 1, ID, 6, BusinessError.newBuilder().setCode("bad").build().toByteString()));
        assertThrows(IllegalArgumentException.class,
                () -> new WireFrame(WireFrame.PUSH, WireRoutes.MAP_VIEW_UPDATE, ID, 0, ByteString.EMPTY));
    }

    @Test void boundsAccountForHeaderAndRejectUnsignedHugeLengthsWithoutAllocating() {
        assertEquals(65_562, GatewayWire.MAX_CLIENT_FRAME_BYTES);
        assertEquals(1_048_602, GatewayWire.MAX_SERVER_FRAME_BYTES);
        for (long invalid : new long[] { -1, 0, 25, 65_563, 0xffff_ffffL })
            assertThrows(IllegalArgumentException.class, () -> GatewayWire.validateLength(invalid, GatewayWire.MAX_CLIENT_FRAME_BYTES));
        var boundary = new WireFrame(WireFrame.REQUEST, 1, ID, 0, ByteString.copyFrom(new byte[RpcTransport.MAX_REQUEST_BYTES]));
        assertEquals(GatewayWire.MAX_CLIENT_FRAME_BYTES + 4, GatewayWire.encode(boundary).length);
        var oversize = new WireFrame(WireFrame.REQUEST, 1, ID, 0, ByteString.copyFrom(new byte[RpcTransport.MAX_REQUEST_BYTES + 1]));
        assertThrows(IllegalArgumentException.class, () -> GatewayWire.encode(oversize));
        var serverBoundary = new WireFrame(WireFrame.RESPONSE, 1, ID, 0, ByteString.copyFrom(new byte[RpcTransport.MAX_RESPONSE_BYTES]));
        assertEquals(serverBoundary, roundtrip(serverBoundary));
    }

    @Test void errorResultHasAnExplicitStatusAndACommonTypedErrorBody() throws Exception {
        BusinessError error = BusinessError.newBuilder().setCode("INVALID_TARGET").setMessage("Mine missing").build();
        var frame = roundtrip(WireFrame.failure(WireRoutes.START_MARCH, ID, WireStatus.REJECTED, error));
        assertEquals(WireStatus.REJECTED, frame.status());
        assertEquals(error, BusinessError.parseFrom(frame.body()));
        assertThrows(IllegalArgumentException.class, () -> WireFrame.failure(1, ID, WireStatus.REJECTED, BusinessError.getDefaultInstance()));
        assertThrows(IllegalArgumentException.class, () -> new WireFrame(WireFrame.RESPONSE, 1, ID, WireStatus.REJECTED, ByteString.fromHex("80")));
        assertThrows(IllegalArgumentException.class, () -> WireFrame.failure(1, ID, WireStatus.OK, error));
    }

    @Test void overloadAndCommitUnknownCannotMasqueradeAsDurableBusinessRejection() {
        assertEquals(WireStatus.NOT_ACCEPTED, status(200, "DISK_PRESSURE"));
        assertEquals(WireStatus.NOT_ACCEPTED, status(200, "PROJECTION_BACKLOG"));
        assertEquals(WireStatus.NOT_ACCEPTED, status(503, "SERVER_BUSY"));
        assertEquals(WireStatus.REJECTED, status(200, "INVALID_TARGET"));
        assertEquals(WireStatus.OUTCOME_UNKNOWN, status(503, "RESULT_UNKNOWN"));
        assertEquals(WireStatus.OUTCOME_UNKNOWN, status(503, "STORAGE_UNAVAILABLE"));
        assertEquals(WireStatus.OUTCOME_UNKNOWN, status(503, "SOME_FUTURE_ERROR"));
        assertEquals(WireStatus.AUTH_REQUIRED, status(401, "UNAUTHORIZED"));
        assertEquals(WireStatus.PROTOCOL_ERROR, status(400, "INVALID_REQUEST"));
        assertEquals(WireStatus.OK, WireStatus.fromReply(PlayReply.newBuilder().setStatus(200).setBody(ByteString.EMPTY).build()));
        assertThrows(IllegalArgumentException.class, () -> WireStatus.fromReply(PlayReply.newBuilder().setStatus(503).setBody(ByteString.EMPTY).build()));
    }

    private static int status(int internal, String code) {
        return WireStatus.fromReply(PlayReply.newBuilder().setStatus(internal).setError(BusinessError.newBuilder().setCode(code)).build());
    }
    private static WireFrame roundtrip(WireFrame frame) {
        byte[] encoded = GatewayWire.encode(frame);
        return GatewayWire.decode(Arrays.copyOfRange(encoded, 4, encoded.length));
    }
}
