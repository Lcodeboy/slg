package com.slg.gate;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import com.slg.core.auth.TokenService;
import com.slg.core.http.RateLimiter;
import com.slg.rpc.generated.GameplayRoutes;
import com.slg.rpc.generated.WireRoutes;
import com.slg.rpc.GatewayWire;
import com.slg.rpc.WireFrame;
import com.slg.rpc.WireStatus;
import com.slg.rpc.RpcMetadata;
import com.slg.rpc.RpcTransport;
import com.slg.rpc.v1.*;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.timeout.IdleStateEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Files;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(25)
class GatewayServerTest {
    private static final InetSocketAddress LOOPBACK = new InetSocketAddress("127.0.0.1", 0);
    private static final String UNUSED = "127.0.0.1:1";
    private final MutableClock clock = new MutableClock();
    private final TokenService tokens = new TokenService("gateway-transport-test-secret-at-least-32-bytes", clock);

    static UUID id(String name) { return UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8)); }
    static WireFrame request(int msgId, String name, MessageLite message) { return WireFrame.request(msgId, id(name), message); }
    private WireFrame auth() {
        return request(WireRoutes.AUTH, "auth", GatewayAuth.newBuilder().setToken(tokens.issue("101", Duration.ofMinutes(5))).build());
    }
    private WireFrame command(String name) { return request(GameplayRoutes.GET_PLAYER, name, GetPlayerReq.getDefaultInstance()); }
    private WireFrame ping(int nonce) {
        return request(WireRoutes.HEARTBEAT, "ping-" + nonce, GatewayHeartbeatRequest.newBuilder().setNonce(nonce).build());
    }
    static BusinessError error(WireFrame frame) throws IOException { return BusinessError.parseFrom(frame.body()); }
    static GatewayConnectionError connectionError(WireFrame frame) throws IOException {
        assertEquals(WireFrame.PUSH, frame.kind());
        assertEquals(WireRoutes.GATEWAY_CONNECTION_ERROR, frame.msgId());
        return GatewayConnectionError.parseFrom(frame.body());
    }

    private EmbeddedChannel authenticated(UpstreamClient upstream, RateLimiter requests) throws Exception {
        var channel = new EmbeddedChannel(new GameTcpHandler(tokens, upstream, requests,
                new RateLimiter(1000, 60, 10), GatewaySettings.defaults())) {
            @Override protected SocketAddress remoteAddress0() { return LOOPBACK; }
        };
        channel.writeInbound(auth());
        WireFrame reply = awaitFrame(channel);
        assertEquals(WireRoutes.AUTH, reply.msgId());
        assertEquals(WireFrame.RESPONSE, reply.kind());
        assertEquals(WireStatus.OK, reply.status());
        return channel;
    }

    private UpstreamClient testUpstream(ControlledGrpcChannel wire) {
        return new UpstreamClient(wire, wire, wire, new SessionTestService(tokens).channel(), UpstreamClientTest.settings(3000));
    }

    static WireFrame awaitFrame(EmbeddedChannel channel) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(4);
        while (System.nanoTime() < deadline) {
            channel.runPendingTasks();
            WireFrame value = channel.readOutbound();
            if (value != null) return value;
            Thread.sleep(2);
        }
        throw new AssertionError("Expected asynchronous gateway frame");
    }

    @Test void pingsUseTheirOwnQuotaAndDisconnectSlowReaders() throws Exception {
        try (var upstream = testUpstream(new ControlledGrpcChannel())) {
            var business = new RateLimiter(1, 60, 10);
            var channel = authenticated(upstream, business);
            try {
                int budget = GatewaySettings.defaults().controlFramesPerSecond();
                for (int i = 0; i < budget; i++) {
                    channel.writeInbound(ping(i));
                    WireFrame reply = channel.readOutbound();
                    assertEquals(WireFrame.RESPONSE, reply.kind());
                    assertEquals(id("ping-" + i), reply.requestId());
                    assertEquals(i, GatewayHeartbeatReply.parseFrom(reply.body()).getNonce());
                }
                assertTrue(business.allow("127.0.0.1"));
                channel.writeInbound(ping(budget));
                assertFalse(channel.isActive());
            } finally { channel.finishAndReleaseAll(); }

            channel = authenticated(upstream, new RateLimiter(1000, 60, 10));
            try {
                channel.unsafe().outboundBuffer().setUserDefinedWritability(1, false);
                channel.writeInbound(ping(1));
                assertFalse(channel.isActive());
                assertNull(channel.readOutbound());
            } finally { channel.finishAndReleaseAll(); }
        }
    }

    @Test void serverHeartbeatChecksRequestIdAndNonceAndDoesNotAcceptOtherResponseTypes() throws Exception {
        try (var upstream = testUpstream(new ControlledGrpcChannel())) {
            var channel = authenticated(upstream, new RateLimiter(1000, 60, 10));
            try {
                channel.pipeline().fireUserEventTriggered(IdleStateEvent.WRITER_IDLE_STATE_EVENT);
                WireFrame ping = channel.readOutbound();
                assertEquals(WireFrame.REQUEST, ping.kind());
                assertEquals(WireRoutes.HEARTBEAT, ping.msgId());
                var body = GatewayHeartbeatRequest.parseFrom(ping.body());
                channel.writeInbound(WireFrame.response(ping.msgId(), ping.requestId(),
                        GatewayHeartbeatReply.newBuilder().setNonce(body.getNonce()).build()));
                assertTrue(channel.isActive());
                assertNull(channel.readOutbound());
                channel.writeInbound(WireFrame.response(ping.msgId(), ping.requestId(),
                        GatewayHeartbeatReply.newBuilder().setNonce(body.getNonce()).build()));
                assertFalse(channel.isActive(), "已经消费的心跳响应不能重复提交");
            } finally { channel.finishAndReleaseAll(); }
        }
    }

    @Test void secondCommandIsRefusedWithItsIdAndDisconnectCancelsGrpcWait() throws Exception {
        var wire = new ControlledGrpcChannel();
        try (var upstream = testUpstream(wire)) {
            var channel = authenticated(upstream, new RateLimiter(1000, 60, 10));
            try {
                channel.writeInbound(command("first"));
                var pending = wire.next();
                assertEquals(id("first").toString(), ((PlayRequest) pending.request()).getRequestId());
                channel.writeInbound(command("second"));
                WireFrame busy = channel.readOutbound();
                assertEquals(id("second"), busy.requestId());
                assertEquals("BUSY", error(busy).getCode());
                assertEquals(WireStatus.NOT_ACCEPTED, busy.status());
                channel.writeInbound(request(GameplayRoutes.START_MARCH, "march", StartMarchReq.getDefaultInstance()));
                WireFrame typedBusy = channel.readOutbound();
                assertEquals("BUSY", error(typedBusy).getCode());
                assertEquals(GameplayRoutes.START_MARCH, typedBusy.msgId());
                assertEquals(1, wire.count.get());
                channel.close();
                assertTrue(pending.future().isCancelled());
                assertEquals(0, UpstreamClientTest.metric(upstream, "game", "inFlight"));
            } finally { channel.finishAndReleaseAll(); }
        }
    }

    @Test void authenticationCompletedAfterTcpCloseReleasesItsRemoteBinding() throws Exception {
        var business = new ControlledGrpcChannel();
        var sessions = new ControlledGrpcChannel();
        try (var upstream = new UpstreamClient(business, business, business, sessions, UpstreamClientTest.settings(3000))) {
            var connection = new EmbeddedChannel(new GameTcpHandler(tokens, upstream,
                    new RateLimiter(1000, 60, 10), new RateLimiter(1000, 60, 10), GatewaySettings.defaults())) {
                @Override protected SocketAddress remoteAddress0() { return LOOPBACK; }
            };
            try {
                connection.writeInbound(auth());
                String instance = UUID.randomUUID().toString();
                sessions.next().reply(SessionPreparation.newBuilder().setGameInstance(instance)
                        .setTicketId(UUID.randomUUID().toString()).build());
                var opening = sessions.next();
                var key = SessionKey.newBuilder().setAccountId("101").setGameInstance(instance).setGeneration(1)
                        .setBindingId(UUID.randomUUID().toString()).build();
                opening.reply(SessionBinding.newBuilder().setKey(key).setLeaseMillis(30_000)
                        .setExpiresAt(clock.instant().plusSeconds(300).getEpochSecond()).build());
                // gRPC 已完成，但 EventLoop 尚未运行认证回调；这时断开也必须释放刚取得的 Lease。
                connection.close();
                connection.runPendingTasks();
                assertEquals(0, upstream.sessions().metrics().get("active"));
                var closing = sessions.next();
                if (closing.request() instanceof SessionCheckRequest) {
                    closing.reply(SessionCheckReply.newBuilder().setLeaseMillis(30_000).build());
                    closing = sessions.next();
                }
                assertEquals(key, ((SessionCloseRequest) closing.request()).getBinding());
                assertEquals(0, business.count.get());
            } finally { connection.finishAndReleaseAll(); }
        }
    }

    @Test void gameplayBeforeAuthenticationAcknowledgmentIsRejectedWithoutForwarding() throws Exception {
        var business = new ControlledGrpcChannel();
        var sessions = new ControlledGrpcChannel();
        try (var upstream = new UpstreamClient(business, business, business, sessions, UpstreamClientTest.settings(3000))) {
            var connection = new EmbeddedChannel(new GameTcpHandler(tokens, upstream,
                    new RateLimiter(1000, 60, 10), new RateLimiter(1000, 60, 10), GatewaySettings.defaults())) {
                @Override protected SocketAddress remoteAddress0() { return LOOPBACK; }
            };
            try {
                connection.writeInbound(auth());
                var preparing = sessions.next();
                connection.writeInbound(command("premature"));
                assertEquals("AUTH_IN_PROGRESS", connectionError(connection.readOutbound()).getError().getCode());
                assertFalse(connection.isActive());
                assertTrue(preparing.future().isCancelled());
                assertEquals(0, business.count.get());
                assertEquals(0, upstream.sessions().metrics().get("opening"));
            } finally { connection.finishAndReleaseAll(); }
        }
    }

    @Test void replacedReasonSurvivesAnOldFrameBeforeTheQueuedRevocationCallback() throws Exception {
        var business = new ControlledGrpcChannel();
        var sessions = new ControlledGrpcChannel();
        try (var upstream = new UpstreamClient(business, business, business, sessions, UpstreamClientTest.settings(3000))) {
            var channel = new EmbeddedChannel(new GameTcpHandler(tokens, upstream, new RateLimiter(1000, 60, 10),
                    new RateLimiter(1000, 60, 10), GatewaySettings.defaults())) {
                @Override protected SocketAddress remoteAddress0() { return LOOPBACK; }
            };
            try {
                channel.writeInbound(auth());
                sessions.next().reply(SessionPreparation.newBuilder().setGameInstance(SessionTestService.KEY.getGameInstance())
                        .setObservedGeneration(0).setTicketId(UUID.randomUUID().toString()).build());
                sessions.next().reply(SessionBinding.newBuilder().setKey(SessionTestService.KEY)
                        .setExpiresAt(clock.instant().plusSeconds(300).getEpochSecond()).setLeaseMillis(30_000).build());
                assertEquals(WireRoutes.AUTH, awaitFrame(channel).msgId());

                var check = sessions.next();
                assertInstanceOf(SessionCheckRequest.class, check.request());
                check.reply(SessionCheckReply.newBuilder().setLeaseMillis(30_000).build());
                // gRPC 已结束并将撤销通知排到 Embedded EventLoop；这里刻意不 runPendingTasks。
                // 先同步交付旧帧，复现生产 EventLoop 上入站/巡检抢在踢线回调之前执行的窗口。
                assertTrue(channel.isActive());
                assertNull(channel.readOutbound());
                channel.pipeline().fireChannelRead(command("frame-before-session-revocation"));
                assertEquals("SESSION_REPLACED", connectionError(channel.readOutbound()).getError().getCode());
                assertEquals(0, business.count.get(), "A replaced binding cannot forward the old frame");
                channel.runPendingTasks();
                assertFalse(channel.isActive());
                assertNull(channel.readOutbound(), "The delayed callback cannot emit a second terminal frame");
            } finally { channel.finishAndReleaseAll(); }
        }
    }

    @Test void gameplayAdmissionRoutesOnlyDeclaredIdsAndCorrelatesUpstreamReplies() throws Exception {
        var calls = new AtomicInteger();
        var game = RpcTransport.serverBuilder(LOOPBACK).addService(new SessionTestService(tokens)).addService(new GameRpcGrpc.GameRpcImplBase() {
            @Override public void play(PlayRequest in, StreamObserver<PlayReply> out) {
                calls.incrementAndGet();
                // 不完整 PB 也原样回显，验证 Gateway 只读取包头，不重复解析 GAME 正文。
                var reply = PlayReply.newBuilder().setMsgId(in.getMsgId()).setRequestId(in.getRequestId())
                        .setStatus(200).setBody(in.getBody());
                if (in.getRequestId().equals(id("wrong-reply").toString())) reply.setMsgId(0);
                out.onNext(reply.build()); out.onCompleted();
            }
        }).build().start();
        try (var gate = new GatewayServer(UNUSED, "127.0.0.1:" + game.getPort(), tokens)) {
            gate.start(LOOPBACK, LOOPBACK);
            try (var socket = connect(gate.tcpPort())) {
                socket.getOutputStream().write(packet(auth()));
                assertEquals(WireRoutes.AUTH, read(socket).msgId());
                var request = new WireFrame(WireFrame.REQUEST, GameplayRoutes.START_MARCH, id("order-17"), 0,
                        ByteString.copyFrom(new byte[]{(byte) 255}));
                socket.getOutputStream().write(packet(request));
                var ok = read(socket);
                assertEquals(request.body(), ok.body());
                assertEquals(request.requestId(), ok.requestId());
                assertEquals(WireStatus.OK, ok.status());
                socket.getOutputStream().write(packet(new WireFrame(WireFrame.REQUEST, 777777, request.requestId(), 0, request.body())));
                assertEquals("UNKNOWN_MSG_ID", error(read(socket)).getCode());
                assertEquals(1, calls.get());
                socket.getOutputStream().write(packet(new WireFrame(WireFrame.REQUEST, request.msgId(), id("wrong-reply"), 0, request.body())));
                var unknown = read(socket);
                assertEquals("UPSTREAM_UNAVAILABLE_UNKNOWN", error(unknown).getCode());
                assertEquals(WireStatus.OUTCOME_UNKNOWN, unknown.status());
                assertEquals(2, calls.get());
            }
        } finally { game.shutdownNow().awaitTermination(3, TimeUnit.SECONDS); }
    }

    @Test void externallyValidBodyThatExceedsInternalEnvelopeBudgetNeverReachesGame() throws Exception {
        var wire = new ControlledGrpcChannel();
        try (var upstream = testUpstream(wire)) {
            var channel = authenticated(upstream, new RateLimiter(1000, 60, 10));
            try {
                var request = new WireFrame(WireFrame.REQUEST, GameplayRoutes.START_MARCH, id("too-large-internal"),
                        0, ByteString.copyFrom(new byte[RpcTransport.MAX_REQUEST_BYTES]));
                assertNotNull(GatewayWire.encode(request), "外部包本身允许该正文长度");
                channel.writeInbound(request);
                WireFrame reply = channel.readOutbound();
                assertEquals(WireStatus.PROTOCOL_ERROR, reply.status());
                assertEquals("PAYLOAD_TOO_LARGE", error(reply).getCode());
                assertEquals(0, wire.count.get());
                assertTrue(channel.isActive());
            } finally { channel.finishAndReleaseAll(); }
        }
    }

    @Test void reconnectRetryPreservesCanonicalRequestIdAndOriginalBody() throws Exception {
        var seen = new java.util.concurrent.LinkedBlockingQueue<PlayRequest>();
        var game = RpcTransport.serverBuilder(LOOPBACK).addService(new SessionTestService(tokens)).addService(new GameRpcGrpc.GameRpcImplBase() {
            @Override public void play(PlayRequest in, StreamObserver<PlayReply> out) {
                seen.add(in);
                out.onNext(PlayReply.newBuilder().setMsgId(in.getMsgId()).setRequestId(in.getRequestId())
                        .setStatus(200).setBody(ByteString.EMPTY).build()); out.onCompleted();
            }
        }).build().start();
        try (var gate = new GatewayServer(UNUSED, "127.0.0.1:" + game.getPort(), tokens)) {
            gate.start(LOOPBACK, LOOPBACK);
            var original = command("retry-keeps-receipt-key");
            for (int i = 0; i < 2; i++) try (var socket = connect(gate.tcpPort())) {
                socket.getOutputStream().write(packet(auth())); read(socket);
                socket.getOutputStream().write(packet(original));
                assertEquals(original.requestId(), read(socket).requestId());
            }
            var first = seen.poll(1, TimeUnit.SECONDS);
            assertNotNull(first);
            assertEquals(first, seen.poll(1, TimeUnit.SECONDS), "重连不得重生成回执 UUID 或改变请求摘要输入");
        } finally { game.shutdownNow().awaitTermination(3, TimeUnit.SECONDS); }
    }

    @Test void admissionIsSharedAcrossBothListenersAndReleasedAfterDisconnect() throws Exception {
        var settings = new GatewaySettings(1, 6000, 60, 30, 10, 3000, 1, 1);
        try (var gate = new GatewayServer(UNUSED, UNUSED, tokens, settings)) {
            gate.start(LOOPBACK, LOOPBACK);
            try (var first = connect(gate.tcpPort())) {
                awaitConnections(gate, 1);
                try (var extra = connect(gate.port())) { assertEquals(-1, extra.getInputStream().read()); }
                assertEquals(1L, gate.health().get("rejectedConnections"));
            }
            awaitConnections(gate, 0);
            try (var next = connect(gate.tcpPort())) { awaitConnections(gate, 1); }
        }
    }

    @Test void streamBoundariesAuthAndGrpcForwardingOnRealSockets() throws Exception {
        var count = new AtomicInteger();
        var deadline = new AtomicLong();
        var game = RpcTransport.serverBuilder(LOOPBACK).addService(new SessionTestService(tokens)).addService(new GameRpcGrpc.GameRpcImplBase() {
            @Override public void play(PlayRequest request, StreamObserver<PlayReply> out) {
                count.incrementAndGet(); tokens.verify(RpcMetadata.TOKEN.get());
                assertNotNull(RpcMetadata.SESSION.get(), "Gateway 必须附加绑定代次，不能只转发登录凭证");
                assertEquals("101", RpcMetadata.SESSION.get().getAccountId());
                deadline.set(Context.current().getDeadline().timeRemaining(TimeUnit.MILLISECONDS));
                var player = GetPlayerRsp.newBuilder().setPlayer(PlayerView.newBuilder().setAccountId("101")).build();
                out.onNext(PlayReply.newBuilder().setMsgId(request.getMsgId()).setRequestId(request.getRequestId())
                        .setStatus(200).setBody(player.toByteString()).build()); out.onCompleted();
            }
        }).build().start();
        String target = "127.0.0.1:" + game.getPort();
        try (var gate = new GatewayServer(target, target, tokens); var http = HttpClient.newHttpClient()) {
            gate.start(LOOPBACK, LOOPBACK);
            String base = "http://127.0.0.1:" + gate.port();
            assertEquals(200, http.send(HttpRequest.newBuilder(URI.create(base + "/health")).GET().build(),
                    HttpResponse.BodyHandlers.discarding()).statusCode());
            for (String removed : List.of("/ws", "/api/game/command"))
                assertEquals(404, http.send(HttpRequest.newBuilder(URI.create(base + removed)).POST(HttpRequest.BodyPublishers.noBody()).build(),
                        HttpResponse.BodyHandlers.discarding()).statusCode());
            try (var socket = connect(gate.tcpPort())) {
                byte[] first = packet(auth());
                socket.getOutputStream().write(first, 0, 2);
                var rest = new ByteArrayOutputStream();
                rest.write(first, 2, first.length - 2);
                socket.getOutputStream().write(rest.toByteArray());
                assertEquals("101", GatewayAuthOk.parseFrom(read(socket).body()).getAccountId());
                // 绑定为异步控制事务，客户端必须收到 AUTH_OK 才能发送心跳或玩法。
                socket.getOutputStream().write(packet(ping(27)));
                assertEquals(27, GatewayHeartbeatReply.parseFrom(read(socket).body()).getNonce());
                socket.getOutputStream().write(packet(command("normal")));
                var result = read(socket);
                assertEquals(id("normal"), result.requestId());
                assertEquals("101", GetPlayerRsp.parseFrom(result.body()).getPlayer().getAccountId());
                assertTrue(deadline.get() > 0 && deadline.get() <= 3000);
                clock.advance(Duration.ofMinutes(6));
                socket.getOutputStream().write(packet(command("expired")));
                assertEquals("UNAUTHORIZED", connectionError(read(socket)).getError().getCode());
                assertEquals(-1, socket.getInputStream().read());
                assertEquals(1, count.get());
            }
        } finally { game.shutdownNow().awaitTermination(3, TimeUnit.SECONDS); }
    }

    @Test void protocolErrorsCannotReachGameAndInvalidLengthIsRejectedBeforeReadingBody() throws Exception {
        var sessionServer = RpcTransport.serverBuilder(LOOPBACK).addService(new SessionTestService(tokens)).build().start();
        try (var gate = new GatewayServer(UNUSED, "127.0.0.1:" + sessionServer.getPort(), tokens)) {
            gate.start(LOOPBACK, LOOPBACK);
            for (var invalid : List.of(command("before-auth"), request(WireRoutes.AUTH, "bad-auth",
                    GatewayAuth.newBuilder().setToken("invalid").build()))) {
                try (var socket = connect(gate.tcpPort())) {
                    socket.getOutputStream().write(packet(invalid));
                    connectionError(read(socket));
                    assertEquals(-1, socket.getInputStream().read());
                }
            }
            // 非法版本、种类、UUID 和状态在解码层直接断连，不能继续在错误帧边界上尝试处理。
            for (int mutation = 0; mutation < 5; mutation++) {
                byte[] invalid = packet(auth());
                switch (mutation) {
                    case 0 -> invalid[4] = 2;
                    case 1 -> invalid[5] = 99;
                    case 2 -> java.util.Arrays.fill(invalid, 10, 26, (byte) 0);
                    case 3 -> ByteBuffer.wrap(invalid).putInt(26, 200);
                    case 4 -> ByteBuffer.wrap(invalid).putInt(6, 0);
                }
                try (var socket = connect(gate.tcpPort())) {
                    socket.getOutputStream().write(invalid);
                    assertEquals(-1, socket.getInputStream().read());
                }
            }
            try (var socket = connect(gate.tcpPort())) {
                socket.getOutputStream().write(packet(auth())); read(socket);
                socket.getOutputStream().write(packet(auth()));
                connectionError(read(socket));
                assertEquals(-1, socket.getInputStream().read());
            }
            for (int length : List.of(0, 25, GatewayWire.MAX_CLIENT_FRAME_BYTES + 1, -1)) {
                try (var socket = connect(gate.tcpPort())) {
                    new DataOutputStream(socket.getOutputStream()).writeInt(length);
                    assertEquals(-1, socket.getInputStream().read(), "非法长度无需等待正文即可拒绝");
                }
            }
        } finally { sessionServer.shutdownNow().awaitTermination(3, TimeUnit.SECONDS); }
    }

    @Test void tlsSocketAuthenticatesAfterCheckingServerCertificateAndRejectsWrongTrustRoot() throws Exception {
        Path dir = Path.of("..", "slg_rpc", "src", "test", "resources", "tls");
        var tls = GatewayServer.tcpTls(Map.of("SLG_GATE_TCP_TLS_CERT", dir.resolve("peer.pem").toString(),
                "SLG_GATE_TCP_TLS_KEY", dir.resolve("peer-key.pem").toString()));
        var sessionServer = RpcTransport.serverBuilder(LOOPBACK).addService(new SessionTestService(tokens)).build().start();
        try (var gate = new GatewayServer(UNUSED, "127.0.0.1:" + sessionServer.getPort(), tokens)) {
            gate.start(LOOPBACK, LOOPBACK, tls);
            try (var trusted = tlsSocket(gate.tcpPort(), dir.resolve("ca.pem"))) {
                trusted.startHandshake(); trusted.getOutputStream().write(packet(auth()));
                assertEquals("101", GatewayAuthOk.parseFrom(read(trusted).body()).getAccountId());
            }
            try (var untrusted = tlsSocket(gate.tcpPort(), dir.resolve("game-ca.pem"))) {
                assertThrows(SSLHandshakeException.class, untrusted::startHandshake);
            }
        } finally { sessionServer.shutdownNow().awaitTermination(3, TimeUnit.SECONDS); }
    }

    @Test void refusesPlaintextOffLoopbackAndRequiresBothTlsFiles() {
        assertThrows(IllegalArgumentException.class, () -> GatewayServer.tcpTls(Map.of("SLG_GATE_TCP_TLS_CERT", "cert")));
        assertThrows(IllegalArgumentException.class, () -> GatewayServer.tcpTls(Map.of("SLG_GATE_TCP_TLS_KEY", "key")));
        try (var gate = new GatewayServer(UNUSED, UNUSED, tokens)) {
            assertThrows(IllegalArgumentException.class, () -> gate.start(LOOPBACK, new InetSocketAddress("0.0.0.0", 0), null));
        }
    }

    @Test void ifAccountPortIsOccupiedFailedStartupAlsoClosesTheGameplayListener() throws Exception {
        try (var occupied = new ServerSocket(); var gate = new GatewayServer(UNUSED, UNUSED, tokens)) {
            occupied.bind(LOOPBACK);
            InetSocketAddress gameplay;
            try (var unused = new ServerSocket()) { unused.bind(LOOPBACK); gameplay = (InetSocketAddress) unused.getLocalSocketAddress(); }
            assertThrows(IllegalStateException.class, () -> gate.start((InetSocketAddress) occupied.getLocalSocketAddress(), gameplay, null));
            try (var replacement = new ServerSocket()) { replacement.bind(gameplay); }
        }
    }

    private static SSLSocket tlsSocket(int port, Path ca) throws Exception {
        var roots = KeyStore.getInstance(KeyStore.getDefaultType()); roots.load(null, null);
        try (var pem = Files.newInputStream(ca)) { roots.setCertificateEntry("root", CertificateFactory.getInstance("X.509").generateCertificate(pem)); }
        var trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()); trust.init(roots);
        var tls = SSLContext.getInstance("TLS"); tls.init(null, trust.getTrustManagers(), null);
        var socket = (SSLSocket) tls.getSocketFactory().createSocket("localhost", port);
        var params = socket.getSSLParameters(); params.setEndpointIdentificationAlgorithm("HTTPS"); socket.setSSLParameters(params);
        socket.setSoTimeout(5000); return socket;
    }
    private static Socket connect(int port) throws IOException {
        var socket = new Socket("127.0.0.1", port); socket.setSoTimeout(5000); return socket;
    }
    private static byte[] packet(WireFrame frame) { return GatewayWire.encode(frame); }
    private static WireFrame read(Socket socket) throws IOException {
        var in = new DataInputStream(socket.getInputStream()); int size = in.readInt();
        assertTrue(size >= GatewayWire.REMAINDER_HEADER_BYTES && size <= GatewayWire.MAX_SERVER_FRAME_BYTES);
        byte[] body = new byte[size]; in.readFully(body); return GatewayWire.decode(body);
    }
    private static void awaitConnections(GatewayServer gateway, int expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!Integer.valueOf(expected).equals(gateway.health().get("connections")) && System.nanoTime() < deadline) Thread.sleep(10);
        assertEquals(expected, gateway.health().get("connections"));
    }
    private static final class MutableClock extends Clock {
        private volatile Instant now = Instant.now();
        void advance(Duration delta) { now = now.plus(delta); }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }
}
