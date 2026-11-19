package com.slg.game;

import com.slg.aoi.AoiRuntime;
import com.slg.aoi.MapStateSource.*;
import com.slg.core.world.Viewport;
import com.slg.gate.GatewayServer;
import com.slg.rpc.GatewayWire;
import com.slg.rpc.WireFrame;
import com.slg.rpc.generated.WireRoutes;
import com.google.protobuf.MessageLite;
import com.slg.rpc.RpcTransport;
import com.slg.rpc.v1.*;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** 有界地图端口→AOI→真实地图 RPC→Gateway→TCP；会话经过真实 PlayerApplication。 */
class AoiGatewayFlowTest {

    @Test void tcpObserverQueriesWatchesReturningAndRemovalWithoutBlockingHeartbeat() throws Exception {
        var source = new MapSourceFixture(1, UUID.randomUUID());
        var march = new Route("tcp-march", 1, Direction.OUTBOUND, 0, 0, 1150, 1150, 1000, 20000);
        source.advance(List.of(), List.of(), List.of(new City("1_101", 1, 0, 0, "A")));
        source.advance(List.of(), List.of(), List.of(new City("1_102", 1, 0, 0, "B")));
        source.advance(List.of(march), List.of(), List.of());
        try (var fixture = new PlayerApplicationFixture();
             var aoi = new AoiRuntime(source, AoiRuntime.Settings.defaults());
             var rpc = new AoiRpcService(aoi, fixture.tokens, () -> true, source::allows, fixture.sessions)) {
            var tokens = fixture.tokens;
            aoi.start();
            await(aoi, 3);
            var server = RpcTransport.serverBuilder(new InetSocketAddress("127.0.0.1", 0)).addService(rpc)
                    .addService(fixture.controls).build().start();
            String target = "127.0.0.1:" + server.getPort();
            try (var gateway = new GatewayServer(target, target, tokens)) {
                gateway.start(new InetSocketAddress("127.0.0.1", 0), new InetSocketAddress("127.0.0.1", 0));
                try (var socket = new Socket("127.0.0.1", gateway.tcpPort())) {
                    socket.setSoTimeout(8000);
                    var input = new DataInputStream(socket.getInputStream());
                    var output = new DataOutputStream(socket.getOutputStream());
                    send(output, WireRoutes.AUTH, GatewayAuth.newBuilder().setToken(tokens.issue("102", Duration.ofHours(1))).build());
                    assertEquals(WireRoutes.AUTH, receive(input).msgId());
                    var viewport = MapViewport.newBuilder().setMinX(550).setMinY(550).setMaxX(650).setMaxY(650);
                    send(output, WireRoutes.MAP_QUERY, MapQueryRequest.newBuilder().setViewport(viewport).build());
                    var query = MapQueryReply.parseFrom(receive(input).body());
                    assertTrue(query.hasSnapshot(), query.toString());
                    assertEquals(march.marchId(), query.getSnapshot().getRoutes(0).getMarchId());

                    send(output, WireRoutes.MAP_WATCH, MapWatchRequest.newBuilder().setSubscriptionId("view-1").setViewport(viewport).build());
                    var accepted = receive(input);
                    assertEquals(WireRoutes.MAP_WATCH, accepted.msgId());
                    assertEquals("view-1", MapWatchReply.parseFrom(accepted.body()).getSubscriptionId());
                    var snapshot = update(input);
                    assertTrue(snapshot.getSnapshot(), snapshot.toString());
                    assertEquals(3, snapshot.getThroughSeq());
                    source.advance(List.of(new Route(march.marchId(), 2, Direction.RETURNING,
                            900, 900, 0, 0, 15000, 30000)), List.of(), List.of());
                    var recall = update(input);
                    assertFalse(recall.hasError(), recall.toString());
                    assertEquals("view-1", recall.getSubscriptionId());
                    assertEquals(snapshot.getGeneration(), recall.getGeneration());
                    assertEquals(3, recall.getFromSeq());
                    assertEquals(4, recall.getThroughSeq());
                    assertEquals(MarchState.MARCH_STATE_RETURNING, recall.getUpserts(0).getState());

                    // 长期地图流不占玩法/心跳通道；TCP 写出地图包之后 Gateway 才向上游申请下一包。
                    send(output, WireRoutes.HEARTBEAT, GatewayHeartbeatRequest.newBuilder().setNonce(42).build());
                    assertEquals(42, GatewayHeartbeatReply.parseFrom(receive(input).body()).getNonce());
                    source.advance(List.of(), List.of(march.marchId()), List.of());
                    var returned = update(input);
                    assertEquals(5, returned.getThroughSeq());
                    assertEquals(march.marchId(), returned.getRemovedIds(0));
                    send(output, WireRoutes.MAP_CANCEL, MapWatchCancel.newBuilder().setSubscriptionId("view-1").build());
                    assertEquals(WireRoutes.MAP_CANCEL, receive(input).msgId());
                    send(output, WireRoutes.MAP_QUERY, MapQueryRequest.newBuilder().setViewport(viewport).build());
                    var empty = MapQueryReply.parseFrom(receive(input).body());
                    assertTrue(empty.hasSnapshot(), empty.toString());
                    assertEquals(0, empty.getSnapshot().getRoutesCount());
                }
            } finally { server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS); }
        }
    }

    private static void send(DataOutputStream output, int msgId, MessageLite body) throws Exception {
        output.write(GatewayWire.encode(WireFrame.request(msgId, UUID.randomUUID(), body)));
        output.flush();
    }
    private static WireFrame receive(DataInputStream input) throws Exception {
        int length = input.readInt();
        assertTrue(length >= GatewayWire.REMAINDER_HEADER_BYTES && length <= GatewayWire.MAX_SERVER_FRAME_BYTES);
        byte[] body = input.readNBytes(length);
        assertEquals(length, body.length);
        var frame = GatewayWire.decode(body);
        assertEquals(0, frame.status());
        return frame;
    }
    private static MapViewUpdate update(DataInputStream input) throws Exception {
        var frame = receive(input);
        assertEquals(WireFrame.PUSH, frame.kind());
        assertEquals(WireRoutes.MAP_VIEW_UPDATE, frame.msgId());
        assertEquals(WireFrame.NO_REQUEST, frame.requestId());
        return MapViewUpdate.parseFrom(frame.body());
    }
    private static void await(AoiRuntime aoi, long sequence) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
        while (System.nanoTime() < deadline) {
            try { if (aoi.query(new Viewport(550, 550, 650, 650)).get(1, TimeUnit.SECONDS).appliedSeq() >= sequence) return; }
            catch (java.util.concurrent.ExecutionException notReady) { }
            Thread.sleep(10);
        }
        fail("AOI did not become ready: " + aoi.metrics());
    }
}
