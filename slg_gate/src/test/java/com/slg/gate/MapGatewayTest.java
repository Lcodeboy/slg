package com.slg.gate;

import com.slg.core.auth.TokenService;
import com.slg.core.http.RateLimiter;
import com.slg.rpc.WireFrame;
import com.slg.rpc.WireStatus;
import com.slg.rpc.generated.GameplayRoutes;
import com.slg.rpc.generated.WireRoutes;
import com.slg.rpc.v1.*;
import io.netty.channel.embedded.EmbeddedChannel;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Clock;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static com.slg.gate.GatewayServerTest.*;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class MapGatewayTest {
    private static final TokenService TOKENS = new TokenService("map-gateway-test-signing-key-with-32-bytes", Clock.systemUTC());
    private static final MapViewport CENTER = MapViewport.newBuilder().setMinX(590).setMinY(590).setMaxX(610).setMaxY(610).build();

    private EmbeddedChannel authenticated(UpstreamClient upstream) throws Exception {
        var channel = new EmbeddedChannel(new GameTcpHandler(TOKENS, upstream,
                new RateLimiter(1000, 60, 10), new RateLimiter(1000, 60, 10), GatewaySettings.defaults())) {
            @Override protected SocketAddress remoteAddress0() { return new InetSocketAddress("127.0.0.1", 12345); }
        };
        channel.writeInbound(request(WireRoutes.AUTH, "auth", GatewayAuth.newBuilder()
                .setToken(TOKENS.issue("101", Duration.ofMinutes(5))).build()));
        assertEquals(WireRoutes.AUTH, awaitFrame(channel).msgId());
        return channel;
    }

    private static WireFrame query(String requestId) {
        return request(WireRoutes.MAP_QUERY, requestId, MapQueryRequest.newBuilder().setViewport(CENTER).build());
    }
    private static WireFrame watch(String subscription) {
        return request(WireRoutes.MAP_WATCH, "watch-" + subscription,
                MapWatchRequest.newBuilder().setSubscriptionId(subscription).setViewport(CENTER).build());
    }
    private static WireFrame cancel(String subscription) {
        return request(WireRoutes.MAP_CANCEL, "cancel-" + subscription,
                MapWatchCancel.newBuilder().setSubscriptionId(subscription).build());
    }
    private static WireFrame ack(EmbeddedChannel connection, int expectedMsgId, String expectedRequestId) {
        WireFrame response = connection.readOutbound();
        assertNotNull(response);
        assertEquals(WireFrame.RESPONSE, response.kind());
        assertEquals(WireStatus.OK, response.status());
        assertEquals(expectedMsgId, response.msgId());
        assertEquals(id(expectedRequestId), response.requestId());
        return response;
    }
    private static MapViewUpdate update(EmbeddedChannel connection) throws Exception {
        WireFrame push = connection.readOutbound();
        assertNotNull(push);
        assertEquals(WireFrame.PUSH, push.kind());
        assertEquals(WireRoutes.MAP_VIEW_UPDATE, push.msgId());
        assertEquals(new java.util.UUID(0, 0), push.requestId());
        assertEquals(WireStatus.OK, push.status());
        return MapViewUpdate.parseFrom(push.body());
    }

    @Test void pendingMapQueryDoesNotBlockGameplayAndBothCancelOnDisconnect() throws Exception {
        var wire = new ControlledGrpcChannel();
        try (var upstream = new UpstreamClient(wire, wire, wire, new SessionTestService(TOKENS).channel(), UpstreamClientTest.settings(3000))) {
            var connection = authenticated(upstream);
            try {
                connection.writeInbound(query("view"));
                var map = wire.next();
                connection.writeInbound(request(GameplayRoutes.GET_PLAYER, "game", GetPlayerReq.getDefaultInstance()));
                var game = wire.next();
                assertInstanceOf(MapQueryRequest.class, map.request());
                assertInstanceOf(PlayRequest.class, game.request());
                connection.writeInbound(query("extra"));
                WireFrame busy = connection.readOutbound();
                assertEquals("MAP_BUSY", error(busy).getCode());
                assertEquals(WireStatus.NOT_ACCEPTED, busy.status());
                assertEquals(id("extra"), busy.requestId());
                connection.close();
                assertTrue(map.future().isCancelled());
                assertTrue(game.future().isCancelled());
            } finally { connection.finishAndReleaseAll(); }
        }
    }

    @Test void viewportReplacementRejectsLateUpdatesAndAcknowledgesBeforeFirstPush() throws Exception {
        var wire = new ControlledGrpcChannel();
        try (var upstream = new UpstreamClient(wire, wire, wire, new SessionTestService(TOKENS).channel(), UpstreamClientTest.settings(3000))) {
            var connection = authenticated(upstream);
            try {
                connection.writeInbound(watch("first"));
                var first = wire.next();
                first.message(MapViewUpdate.newBuilder().setSubscriptionId("first").setSnapshot(true).build());
                connection.runPendingTasks();
                var accepted = ack(connection, WireRoutes.MAP_WATCH, "watch-first");
                assertEquals("first", MapWatchReply.parseFrom(accepted.body()).getSubscriptionId());
                assertEquals("first", update(connection).getSubscriptionId());
                assertEquals(2, first.requested().get(), "写完首包后才允许请求下一条更新");
                connection.writeInbound(watch("second"));
                var second = wire.next();
                assertTrue(first.future().isCancelled());
                first.message(MapViewUpdate.newBuilder().setSubscriptionId("first").setThroughSeq(20).build());
                second.message(MapViewUpdate.newBuilder().setSubscriptionId("second").setSnapshot(true).build());
                connection.runPendingTasks();
                ack(connection, WireRoutes.MAP_WATCH, "watch-second");
                assertEquals("second", update(connection).getSubscriptionId());
                assertNull(connection.readOutbound());
                connection.writeInbound(cancel("first"));
                ack(connection, WireRoutes.MAP_CANCEL, "cancel-first");
                assertFalse(second.future().isCancelled(), "旧视野的迟到取消不能终止新视野");
                connection.writeInbound(cancel("second"));
                var cancelled = ack(connection, WireRoutes.MAP_CANCEL, "cancel-second");
                assertEquals("second", MapCancelReply.parseFrom(cancelled.body()).getSubscriptionId());
                assertTrue(second.future().isCancelled());
                assertTrue(connection.isActive());
            } finally { connection.finishAndReleaseAll(); }
        }
    }

    @Test void fullWatchPoolRejectsBeforeAckAndKeepsQueriesUsable() throws Exception {
        var wire = new ControlledGrpcChannel();
        var settings = new GatewaySettings(100, 6000, 60, 30, 10, 3000, 1, 1, 1, 1);
        try (var upstream = new UpstreamClient(wire, wire, wire, new SessionTestService(TOKENS).channel(), settings)) {
            var connection = authenticated(upstream);
            try {
                connection.writeInbound(watch("first"));
                ack(connection, WireRoutes.MAP_WATCH, "watch-first");
                var first = wire.next();
                // 旧流已经取消，但 transport 尚未回 onClose，替换请求不能提前借走同一个槽。
                connection.writeInbound(watch("replacement"));
                WireFrame busy = connection.readOutbound();
                assertEquals(WireFrame.RESPONSE, busy.kind());
                assertEquals(WireStatus.NOT_ACCEPTED, busy.status());
                assertEquals("MAP_BUSY", error(busy).getCode());
                assertEquals(id("watch-replacement"), busy.requestId());
                assertNull(connection.readOutbound(), "受理前失败不能再发订阅 ACK 或重同步推送");
                assertTrue(first.future().isCancelled());
                assertTrue(connection.isActive());
                connection.writeInbound(query("while-full"));
                wire.next().reply(MapQueryReply.newBuilder().setSnapshot(MapViewSnapshot.getDefaultInstance()).build());
                connection.runPendingTasks();
                ack(connection, WireRoutes.MAP_QUERY, "while-full");
                first.fail(io.grpc.Status.CANCELLED);
                connection.runPendingTasks();
                assertNull(connection.readOutbound(), "旧流迟到终态不能污染新视野的响应");
                connection.writeInbound(watch("retry-new-id"));
                ack(connection, WireRoutes.MAP_WATCH, "watch-retry-new-id");
                assertEquals("retry-new-id", ((MapWatchRequest) wire.next().request()).getSubscriptionId());
            } finally { connection.finishAndReleaseAll(); }
        }
    }

    @Test void mapAuthorizationFailureIsExplicitAndDoesNotCloseGameplayConnection() throws Exception {
        var wire = new ControlledGrpcChannel();
        try (var upstream = new UpstreamClient(wire, wire, wire, new SessionTestService(TOKENS).channel(), UpstreamClientTest.settings(3000))) {
            var connection = authenticated(upstream);
            try {
                connection.writeInbound(query("uncreated-role"));
                wire.next().fail(io.grpc.Status.PERMISSION_DENIED);
                connection.runPendingTasks();
                WireFrame reply = connection.readOutbound();
                assertEquals("MAP_ACCESS_DENIED", error(reply).getCode());
                assertEquals(WireStatus.REJECTED, reply.status());
                assertTrue(connection.isActive(), "缺少本区角色时仍可在同一会话发建角命令");
            } finally { connection.finishAndReleaseAll(); }
        }
    }

    @Test void alreadyAcceptedWatchFailureUsesPushWithoutASecondResponse() throws Exception {
        var wire = new ControlledGrpcChannel();
        try (var upstream = new UpstreamClient(wire, wire, wire, new SessionTestService(TOKENS).channel(), UpstreamClientTest.settings(3000))) {
            var connection = authenticated(upstream);
            try {
                connection.writeInbound(watch("denied"));
                ack(connection, WireRoutes.MAP_WATCH, "watch-denied");
                wire.next().fail(io.grpc.Status.PERMISSION_DENIED);
                connection.runPendingTasks();
                assertEquals("MAP_RESYNC_REQUIRED", update(connection).getError().getCode());
                assertNull(connection.readOutbound());
                assertTrue(connection.isActive());
            } finally { connection.finishAndReleaseAll(); }
        }
    }

    @Test void slowSocketEndsOnlyMapStreamAndRequiresResyncAfterItBecomesWritable() throws Exception {
        var wire = new ControlledGrpcChannel();
        try (var upstream = new UpstreamClient(wire, wire, wire, new SessionTestService(TOKENS).channel(), UpstreamClientTest.settings(3000))) {
            var connection = authenticated(upstream);
            try {
                connection.writeInbound(watch("slow"));
                ack(connection, WireRoutes.MAP_WATCH, "watch-slow");
                var watch = wire.next();
                connection.unsafe().outboundBuffer().setUserDefinedWritability(1, false);
                watch.message(MapViewUpdate.newBuilder().setSubscriptionId("slow").setSnapshot(true).build());
                connection.runPendingTasks();
                assertTrue(watch.future().isCancelled());
                assertTrue(connection.isActive());
                assertNull(connection.readOutbound(), "不可写时不继续压入地图包");
                connection.unsafe().outboundBuffer().setUserDefinedWritability(1, true);
                connection.runPendingTasks();
                var reset = update(connection);
                assertEquals("MAP_RESYNC_REQUIRED", reset.getError().getCode());
                assertEquals("slow", reset.getSubscriptionId());
                assertTrue(connection.isActive());
            } finally { connection.finishAndReleaseAll(); }
        }
    }
}
