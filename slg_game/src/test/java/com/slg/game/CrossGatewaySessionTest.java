package com.slg.game;

import com.google.protobuf.MessageLite;
import com.slg.aoi.AoiRuntime;
import com.slg.aoi.MapStateSource.*;
import com.slg.gate.GatewayServer;
import com.slg.rpc.GatewayWire;
import com.slg.rpc.RpcMetadata;
import com.slg.rpc.RpcTransport;
import com.slg.rpc.WireFrame;
import com.slg.rpc.generated.WireRoutes;
import com.slg.rpc.v1.*;
import io.grpc.*;
import io.grpc.stub.MetadataUtils;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** 两个真实 Gateway 同时连一个真实 GAME，旧连接保持打开，避免把主动断线误当成会话隔离。 */
class CrossGatewaySessionTest {

    @Test void newGatewayFencesOldCommandsAndRestoresOriginalReceiptAndMap() throws Exception {
        AtomicReference<SessionKey> observed = new AtomicReference<>();
        var source = new MapSourceFixture(1, UUID.randomUUID());
        try (var fixture = new PlayerApplicationFixture()) {
            var tokens = fixture.tokens; var sessions = fixture.sessions;
            try (var aoi = new AoiRuntime(source, AoiRuntime.Settings.defaults());
                 var maps = new AoiRpcService(aoi, tokens, () -> true, source::allows, sessions)) {
                aoi.start();
                var gameplay = fixture.gameplay;
                // 仅在测试服务端记录实际收到的内部绑定，稍后绕过旧 Gateway 再次尝试直达 GAME。
                // 这样证明拒绝发生在权威入口，而不只是客户端或网关主动关闭了 socket。
                var capture = new ServerInterceptor() {
                    public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call, Metadata headers,
                            ServerCallHandler<Q, R> next) {
                        observed.set(RpcMetadata.SESSION.get());
                        return next.startCall(call, headers);
                    }
                };
                var server = RpcTransport.serverBuilder(new InetSocketAddress("127.0.0.1", 0))
                        .addService(ServerInterceptors.intercept(gameplay, capture)).addService(maps)
                        .addService(fixture.controls).build().start();
                String target = "127.0.0.1:" + server.getPort();
                var channel = RpcTransport.channel(target);
                try (var first = new GatewayServer(target, target, tokens);
                     var second = new GatewayServer(target, target, tokens)) {
                    first.start(local(), local()); second.start(local(), local());
                    String token = tokens.issue("101", Duration.ofHours(1));
                    try (var old = new Tcp(first.tcpPort(), token)) {
                        UUID creationId = UUID.randomUUID();
                        var order = CreatePlayerReq.newBuilder().setName("mover").build();
                        var created = CreatePlayerRsp.parseFrom(old.call(WireRoutes.CREATE_PLAYER, creationId, order).body());
                        // 这里显式给入已提交公开 DTO：网络会话和地图恢复不重复证明领域/SQL 事务。
                        source.advance(List.of(), List.of(), List.of(new City("1_101", 1, 0, 0, "mover")));
                        var march = new Route("gateway-route", 1, Direction.OUTBOUND, 0, 0, 1150, 1150, 1000, 20000);
                        source.advance(List.of(march), List.of(), List.of());
                        SessionKey oldKey = observed.get();
                        assertNotNull(oldKey);
                        long indexDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                        while ((long) aoi.metrics().get("appliedSeq") < 2 && System.nanoTime() < indexDeadline) Thread.sleep(5);
                        assertEquals(2L, aoi.metrics().get("appliedSeq"));
                        old.subscribe("before-transfer");
                        assertEquals(march.marchId(), old.update().getUpserts(0).getMarchId());

                        try (var current = new Tcp(second.tcpPort(), token)) {
                            assertEquals(old.auth.getGameInstance(), current.auth.getGameInstance());
                            assertTrue(current.auth.getSessionGeneration() > old.auth.getSessionGeneration());
                            long beforeRejected = fixture.commits.sequence.get();
                            var stale = GameRpcGrpc.newBlockingStub(channel).withDeadlineAfter(3, TimeUnit.SECONDS)
                                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(RpcMetadata.headers(token, oldKey)))
                                    .play(PlayRequest.newBuilder().setRequestId(UUID.randomUUID().toString())
                                            .setMsgId(WireRoutes.CREATE_PLAYER).setBody(order.toByteString()).build());
                            assertEquals(401, stale.getStatus());
                            assertEquals("SESSION_REPLACED", stale.getError().getCode());
                            assertEquals(beforeRejected, fixture.commits.sequence.get(), "旧会话不得产生业务变化或拒绝回执");
                            // 地图同样不能仅依赖旧 Gateway 踢线；旧 proof 直接访问 AOI 也必须拒绝。
                            var staleMap = MapRpcGrpc.newBlockingStub(channel).withDeadlineAfter(3, TimeUnit.SECONDS)
                                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(RpcMetadata.headers(token, oldKey)));
                            var mapDenied = assertThrows(StatusRuntimeException.class, () -> staleMap.query(
                                    MapQueryRequest.newBuilder().setViewport(MapViewport.newBuilder()
                                            .setMinX(550).setMinY(550).setMaxX(650).setMaxY(650)).build()));
                            assertEquals(Status.Code.UNAUTHENTICATED, mapDenied.getStatus().getCode());
                            assertEquals("SESSION_REPLACED", mapDenied.getStatus().getDescription());

                            var receipt = GetReceiptRsp.parseFrom(current.call(WireRoutes.GET_RECEIPT, UUID.randomUUID(),
                                    GetReceiptReq.newBuilder().setTargetRequestId(creationId.toString()).build()).body());
                            assertEquals(created, receipt.getReceipt().getCreated());
                            assertEquals(created, CreatePlayerRsp.parseFrom(current.call(WireRoutes.CREATE_PLAYER, creationId, order).body()));
                            assertEquals(beforeRejected, fixture.commits.sequence.get(), "换网关重试原 UUID 不重复提交");
                            current.subscribe("after-transfer");
                            var restored = current.update();
                            assertTrue(restored.getSnapshot());
                            assertEquals("after-transfer", restored.getSubscriptionId());
                            assertEquals(march.marchId(), restored.getUpserts(0).getMarchId());
                            assertEquals(old.auth.getGameInstance(), current.auth.getGameInstance());

                            source.advance(List.of(new Route(march.marchId(), 2, Direction.RETURNING,
                                    900, 900, 0, 0, 15000, 30000)), List.of(), List.of());
                            var returning = current.update();
                            assertFalse(returning.getSnapshot());
                            assertEquals(MarchState.MARCH_STATE_RETURNING, returning.getUpserts(0).getState());
                            source.advance(List.of(), List.of(march.marchId()), List.of());
                            assertEquals(march.marchId(), current.update().getRemovedIds(0));
                            var finalPlayer = GetPlayerRsp.parseFrom(current.call(WireRoutes.GET_PLAYER, UUID.randomUUID(),
                                    GetPlayerReq.getDefaultInstance()).body()).getPlayer();
                            assertEquals(3000, finalPlayer.getTroopsOrThrow("INFANTRY"));
                            assertEquals(0, finalPlayer.getActiveMarchIdsCount());
                            assertEquals("SESSION_REPLACED", old.closedByServer().getError().getCode());
                        }
                    }
                } finally {
                    channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
                    server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    private static InetSocketAddress local() { return new InetSocketAddress("127.0.0.1", 0); }
    private static class Tcp implements AutoCloseable {
        final Socket socket;
        final DataInputStream input;
        final DataOutputStream output;
        final ArrayDeque<MapViewUpdate> updates = new ArrayDeque<>();
        final GatewayAuthOk auth;
        Tcp(int port, String token) throws Exception {
            socket = new Socket("127.0.0.1", port); socket.setSoTimeout(8000);
            input = new DataInputStream(socket.getInputStream()); output = new DataOutputStream(socket.getOutputStream());
            auth = GatewayAuthOk.parseFrom(call(WireRoutes.AUTH, UUID.randomUUID(), GatewayAuth.newBuilder().setToken(token).build()).body());
            assertFalse(auth.getGameInstance().isBlank()); assertTrue(auth.getSessionGeneration() > 0);
        }
        WireFrame read() throws Exception {
            int size = input.readInt();
            assertTrue(size >= GatewayWire.REMAINDER_HEADER_BYTES && size <= GatewayWire.MAX_SERVER_FRAME_BYTES);
            byte[] bytes = input.readNBytes(size); assertEquals(size, bytes.length);
            return GatewayWire.decode(bytes);
        }
        WireFrame call(int msg, UUID id, MessageLite body) throws Exception {
            output.write(GatewayWire.encode(WireFrame.request(msg, id, body))); output.flush();
            for (;;) {
                var frame = read();
                if (frame.kind() == WireFrame.PUSH && frame.msgId() == WireRoutes.MAP_VIEW_UPDATE) {
                    assertTrue(updates.size() < 16, "TCP fixture push capacity exceeded");
                    updates.add(MapViewUpdate.parseFrom(frame.body())); continue;
                }
                assertEquals(WireFrame.RESPONSE, frame.kind()); assertEquals(msg, frame.msgId());
                assertEquals(id, frame.requestId()); assertEquals(0, frame.status(), frame.toString()); return frame;
            }
        }
        void subscribe(String id) throws Exception {
            call(WireRoutes.MAP_WATCH, UUID.randomUUID(), MapWatchRequest.newBuilder().setSubscriptionId(id)
                    .setViewport(MapViewport.newBuilder().setMinX(550).setMinY(550).setMaxX(650).setMaxY(650)).build());
        }
        MapViewUpdate update() throws Exception {
            if (!updates.isEmpty()) return updates.removeFirst();
            var frame = read(); assertEquals(WireRoutes.MAP_VIEW_UPDATE, frame.msgId());
            var update = MapViewUpdate.parseFrom(frame.body()); assertFalse(update.hasError(), update.toString()); return update;
        }
        GatewayConnectionError closedByServer() throws Exception {
            for (int i = 0; i < 16; i++) {
                var frame = read();
                if (frame.msgId() == WireRoutes.GATEWAY_CONNECTION_ERROR) return GatewayConnectionError.parseFrom(frame.body());
            }
            throw new AssertionError("Old gateway did not close its replaced session");
        }
        public void close() throws Exception { socket.close(); }
    }
}
