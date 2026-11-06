package com.slg.gate;

import com.slg.core.auth.TokenService;
import com.slg.core.http.RateLimiter;
import com.slg.rpc.generated.GameplayRoutes;
import com.slg.rpc.GatewayWire;
import com.slg.rpc.RpcMetadata;
import com.slg.rpc.RpcTransport;
import com.slg.rpc.v1.*;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import io.netty.channel.embedded.EmbeddedChannel;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
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

    private GatewayFrame auth() {
        return GatewayFrame.newBuilder().setVersion(GatewayWire.VERSION).setAuth(GatewayAuth.newBuilder()
                .setToken(tokens.issue("101", Duration.ofMinutes(5)))).build();
    }

    private GatewayFrame command(String id) {
        return GatewayFrame.newBuilder().setVersion(GatewayWire.VERSION).setPlayRequest(PlayRequest.newBuilder()
                .setMsgId(GameplayRoutes.GET_PLAYER).setRequestId(id).setBody(GetPlayerReq.getDefaultInstance().toByteString())).build();
    }

    private GatewayFrame ping(int nonce) {
        return GatewayFrame.newBuilder().setVersion(GatewayWire.VERSION).setPing(GatewayHeartbeat.newBuilder().setNonce(nonce)).build();
    }

    private EmbeddedChannel authenticated(UpstreamClient upstream, RateLimiter requests) {
        var channel = new EmbeddedChannel(new GameTcpHandler(tokens, upstream, requests,
                new RateLimiter(1000, 60, 10), GatewaySettings.defaults())) {
            @Override protected SocketAddress remoteAddress0() { return LOOPBACK; }
        };
        channel.writeInbound(auth());
        GatewayFrame reply = channel.readOutbound();
        assertTrue(reply.hasAuthOk());
        return channel;
    }

    @Test void pingsUseTheirOwnQuotaAndDisconnectSlowReaders() {
        try (var upstream = new UpstreamClient(UNUSED, UNUSED)) {
            var business = new RateLimiter(1, 60, 10);
            var channel = authenticated(upstream, business);
            try {
                int budget = GatewaySettings.defaults().controlFramesPerSecond();
                for (int i = 0; i < budget; i++) {
                    channel.writeInbound(ping(i));
                    GatewayFrame reply = channel.readOutbound();
                    assertTrue(reply.hasPong());
                    assertEquals(i, reply.getPong().getNonce());
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

    @Test void secondCommandIsRefusedWithItsIdAndDisconnectCancelsGrpcWait() throws Exception {
        var wire = new ControlledGrpcChannel();
        try (var upstream = new UpstreamClient(wire, wire, UpstreamClientTest.settings(3000))) {
            var channel = authenticated(upstream, new RateLimiter(1000, 60, 10));
            try {
                channel.writeInbound(command("first"));
                var pending = wire.next();
                channel.writeInbound(command("second"));
                GatewayFrame busy = channel.readOutbound();
                assertEquals("second", busy.getPlayReply().getRequestId());
                assertEquals("BUSY", busy.getPlayReply().getError().getCode());
                channel.writeInbound(GatewayFrame.newBuilder().setVersion(GatewayWire.VERSION).setPlayRequest(PlayRequest.newBuilder()
                        .setRequestId("march").setMsgId(GameplayRoutes.START_MARCH)).build());
                GatewayFrame typedBusy = channel.readOutbound();
                assertEquals("BUSY", typedBusy.getPlayReply().getError().getCode());
                assertEquals(GameplayRoutes.START_MARCH, typedBusy.getPlayReply().getMsgId());
                assertEquals(1, wire.count.get());
                channel.close();
                assertTrue(pending.future().isCancelled());
                assertEquals(0, UpstreamClientTest.metric(upstream, "game", "inFlight"));
            } finally { channel.finishAndReleaseAll(); }
        }
    }

    @Test void gameplayAdmissionRoutesOnlyDeclaredIdsAndCorrelatesUpstreamReplies() throws Exception {
        var calls = new AtomicInteger();
        int marchId = GameplayRoutes.START_MARCH;
        var game = RpcTransport.serverBuilder(LOOPBACK).addService(new GameRpcGrpc.GameRpcImplBase() {
            @Override public void play(PlayRequest in, StreamObserver<PlayReply> out) {
                calls.incrementAndGet();
                // 收到不完整的玩法字节也照样回显，用于证明 Gate 只处理包头，由 GAME 负责正文校验。
                var reply = PlayReply.newBuilder().setMsgId(in.getMsgId()).setRequestId(in.getRequestId())
                        .setStatus(200).setBody(in.getBody());
                if (in.getRequestId().equals("wrong-reply")) reply.setMsgId(0);
                out.onNext(reply.build());
                out.onCompleted();
            }
        }).build().start();
        try (var gate = new GatewayServer(UNUSED, "127.0.0.1:" + game.getPort(), tokens)) {
            gate.start(LOOPBACK, LOOPBACK);
            try (var socket = connect(gate.tcpPort())) {
                socket.getOutputStream().write(packet(auth()));
                assertTrue(read(socket).hasAuthOk());
                var request = PlayRequest.newBuilder().setMsgId(marchId).setRequestId("order-17")
                        .setBody(com.google.protobuf.ByteString.copyFrom(new byte[]{(byte) 255}));
                var frame = GatewayFrame.newBuilder().setVersion(GatewayWire.VERSION);
                socket.getOutputStream().write(packet(frame.setPlayRequest(request).build()));
                var ok = read(socket).getPlayReply();
                assertEquals(request.getBody(), ok.getBody());
                assertEquals(request.getRequestId(), ok.getRequestId());
                assertEquals(marchId, ok.getMsgId());
                socket.getOutputStream().write(packet(frame.setPlayRequest(request.setMsgId(0)).build()));
                assertEquals("UNKNOWN_MSG_ID", read(socket).getPlayReply().getError().getCode());
                assertEquals(1, calls.get());
                socket.getOutputStream().write(packet(frame.setPlayRequest(request.setMsgId(marchId)
                        .setRequestId("wrong-reply")).build()));
                assertEquals("UPSTREAM_UNAVAILABLE_UNKNOWN", read(socket).getPlayReply().getError().getCode());
                assertEquals(2, calls.get());
            }
        } finally { game.shutdownNow().awaitTermination(3, TimeUnit.SECONDS); }
    }

    @Test void admissionIsSharedAcrossBothListenersAndReleasedAfterDisconnect() throws Exception {
        var settings = new GatewaySettings(1, 6000, 60, 30, 10, 3000, 1, 1);
        try (var gate = new GatewayServer(UNUSED, UNUSED, tokens, settings)) {
            gate.start(LOOPBACK, LOOPBACK);
            try (var first = connect(gate.tcpPort())) {
                awaitConnections(gate, 1);
                try (var extra = connect(gate.port())) {
                    assertEquals(-1, extra.getInputStream().read());
                }
                assertEquals(1L, gate.health().get("rejectedConnections"));
            }
            awaitConnections(gate, 0);
            try (var next = connect(gate.tcpPort())) { awaitConnections(gate, 1); }
        }
    }

    @Test void streamBoundariesAuthAndGrpcForwardingOnRealSockets() throws Exception {
        var count = new AtomicInteger();
        var deadline = new AtomicLong();
        var game = RpcTransport.serverBuilder(LOOPBACK).addService(new GameRpcGrpc.GameRpcImplBase() {
            @Override public void play(PlayRequest request, StreamObserver<PlayReply> out) {
                count.incrementAndGet();
                tokens.verify(RpcMetadata.TOKEN.get());
                deadline.set(Context.current().getDeadline().timeRemaining(TimeUnit.MILLISECONDS));
                var player = GetPlayerRsp.newBuilder().setPlayer(PlayerView.newBuilder().setAccountId("101")).build();
                out.onNext(PlayReply.newBuilder().setMsgId(request.getMsgId()).setRequestId(request.getRequestId())
                        .setStatus(200).setBody(player.toByteString()).build());
                out.onCompleted();
            }
        }).build().start();
        String target = "127.0.0.1:" + game.getPort();
        try (var gate = new GatewayServer(target, target, tokens); var http = HttpClient.newHttpClient()) {
            gate.start(LOOPBACK, LOOPBACK);
            String base = "http://127.0.0.1:" + gate.port();
            assertEquals(200, http.send(HttpRequest.newBuilder(URI.create(base + "/health")).GET().build(),
                    HttpResponse.BodyHandlers.discarding()).statusCode());
            for (String removed : List.of("/ws", "/api/game/command")) {
                assertEquals(404, http.send(HttpRequest.newBuilder(URI.create(base + removed))
                        .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.discarding()).statusCode());
            }
            try (var socket = connect(gate.tcpPort())) {
                byte[] first = packet(auth());
                socket.getOutputStream().write(first, 0, 2);
                // TCP 可能在长度头中间断开一次 read，也可能一次交付认证帧余部及整个心跳帧。
                var rest = new ByteArrayOutputStream();
                rest.write(first, 2, first.length - 2);
                rest.write(packet(ping(27)));
                socket.getOutputStream().write(rest.toByteArray());
                assertEquals("101", read(socket).getAuthOk().getAccountId());
                assertEquals(27, read(socket).getPong().getNonce());
                String id = "r".repeat(64);
                socket.getOutputStream().write(packet(command(id)));
                var result = read(socket).getPlayReply();
                assertTrue(result.hasBody());
                assertEquals(id, result.getRequestId());
                assertEquals("101", GetPlayerRsp.parseFrom(result.getBody()).getPlayer().getAccountId());
                assertTrue(deadline.get() > 0 && deadline.get() <= 3000);
                socket.getOutputStream().write(packet(command("r".repeat(65))));
                assertFalse(read(socket).getPlayReply().hasBody());
                clock.advance(Duration.ofMinutes(6));
                socket.getOutputStream().write(packet(command("expired")));
                assertTrue(read(socket).hasError());
                assertEquals(-1, socket.getInputStream().read());
                assertEquals(1, count.get());
            }
        } finally { game.shutdownNow().awaitTermination(3, TimeUnit.SECONDS); }
    }

    @Test void protocolErrorsCannotReachGameAndOversizeHeaderIsRejectedBeforeReadingBody() throws Exception {
        try (var gate = new GatewayServer(UNUSED, UNUSED, tokens)) {
            gate.start(LOOPBACK, LOOPBACK);
            for (var invalid : List.of(command("before-auth"), auth().toBuilder().setVersion(1).build(), auth().toBuilder().setVersion(99).build(),
                    GatewayFrame.newBuilder().setVersion(GatewayWire.VERSION).setAuth(GatewayAuth.newBuilder().setToken("invalid")).build())) {
                try (var socket = connect(gate.tcpPort())) {
                    socket.getOutputStream().write(packet(invalid));
                    assertTrue(read(socket).hasError());
                    assertEquals(-1, socket.getInputStream().read());
                }
            }
            try (var socket = connect(gate.tcpPort())) {
                socket.getOutputStream().write(packet(auth()));
                assertTrue(read(socket).hasAuthOk());
                socket.getOutputStream().write(packet(auth()));
                assertTrue(read(socket).hasError());
                assertEquals(-1, socket.getInputStream().read());
            }
            for (int length : List.of(0, GatewayWire.MAX_CLIENT_FRAME_BYTES + 1, -1)) {
                try (var socket = connect(gate.tcpPort())) {
                    new DataOutputStream(socket.getOutputStream()).writeInt(length);
                    // 服务实现允许先发一个有界错误帧；无论是否发送，都应在对方未交付正文时关闭。
                    assertTrue(socket.getInputStream().readNBytes(1024).length < 1024);
                }
            }
        }
    }

    @Test void tlsSocketAuthenticatesAfterCheckingServerCertificateAndRejectsWrongTrustRoot() throws Exception {
        Path dir = Path.of("..", "slg_rpc", "src", "test", "resources", "tls");
        var tls = GatewayServer.tcpTls(Map.of("SLG_GATE_TCP_TLS_CERT", dir.resolve("peer.pem").toString(),
                "SLG_GATE_TCP_TLS_KEY", dir.resolve("peer-key.pem").toString()));
        try (var gate = new GatewayServer(UNUSED, UNUSED, tokens)) {
            gate.start(LOOPBACK, LOOPBACK, tls);
            try (var trusted = tlsSocket(gate.tcpPort(), dir.resolve("ca.pem"))) {
                trusted.startHandshake();
                trusted.getOutputStream().write(packet(auth()));
                assertEquals("101", read(trusted).getAuthOk().getAccountId());
            }
            try (var untrusted = tlsSocket(gate.tcpPort(), dir.resolve("game-ca.pem"))) {
                assertThrows(SSLHandshakeException.class, untrusted::startHandshake);
            }
        }
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
            try (var unused = new ServerSocket()) {
                unused.bind(LOOPBACK);
                gameplay = (InetSocketAddress) unused.getLocalSocketAddress();
            }
            assertThrows(IllegalStateException.class, () -> gate.start(
                    (InetSocketAddress) occupied.getLocalSocketAddress(), gameplay, null));
            // 游戏端口先绑定，随后账号端口失败。只开放一半的网关必须立即退出。
            try (var replacement = new ServerSocket()) { replacement.bind(gameplay); }
        }
    }

    private static SSLSocket tlsSocket(int port, Path ca) throws Exception {
        var roots = KeyStore.getInstance(KeyStore.getDefaultType());
        roots.load(null, null);
        try (var pem = Files.newInputStream(ca)) {
            roots.setCertificateEntry("root", CertificateFactory.getInstance("X.509").generateCertificate(pem));
        }
        var trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trust.init(roots);
        var tls = SSLContext.getInstance("TLS");
        tls.init(null, trust.getTrustManagers(), null);
        var socket = (SSLSocket) tls.getSocketFactory().createSocket("localhost", port);
        var params = socket.getSSLParameters();
        params.setEndpointIdentificationAlgorithm("HTTPS");
        socket.setSSLParameters(params);
        socket.setSoTimeout(5000);
        return socket;
    }

    private static Socket connect(int port) throws IOException {
        var socket = new Socket("127.0.0.1", port);
        socket.setSoTimeout(5000);
        return socket;
    }

    private static byte[] packet(GatewayFrame frame) throws IOException {
        var bytes = new ByteArrayOutputStream();
        new DataOutputStream(bytes).writeInt(frame.getSerializedSize());
        frame.writeTo(bytes);
        return bytes.toByteArray();
    }

    private static GatewayFrame read(Socket socket) throws IOException {
        var in = new DataInputStream(socket.getInputStream());
        int size = in.readInt();
        assertTrue(size > 0 && size <= GatewayWire.MAX_SERVER_FRAME_BYTES);
        byte[] body = new byte[size];
        in.readFully(body);
        var frame = GatewayFrame.parseFrom(body);
        assertEquals(GatewayWire.VERSION, frame.getVersion());
        return frame;
    }

    private static void awaitConnections(GatewayServer gateway, int expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!Integer.valueOf(expected).equals(gateway.health().get("connections")) && System.nanoTime() < deadline)
            Thread.sleep(10);
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
