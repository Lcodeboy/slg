package com.slg.gate;

import com.google.protobuf.ByteString;
import com.slg.core.auth.TokenService;
import com.slg.rpc.RpcTransport;
import com.slg.rpc.generated.GameplayRoutes;
import com.slg.rpc.v1.GameRpcGrpc;
import com.slg.rpc.v1.MapQueryReply;
import com.slg.rpc.v1.MapQueryRequest;
import com.slg.rpc.v1.MapRpcGrpc;
import com.slg.rpc.v1.MapViewSnapshot;
import com.slg.rpc.v1.MapViewUpdate;
import com.slg.rpc.v1.MapWatchRequest;
import com.slg.rpc.v1.PlayReply;
import com.slg.rpc.v1.PlayRequest;
import com.slg.rpc.v1.SessionRpcGrpc;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.*;

/**
 * 用真实 HTTP/2 连接检验池容量及隔离，避免假 Channel 隐藏服务端并发流限制。
 * 对端只返回固定快照；这里证明传输名额与连接生命周期，不代表 GAME/AOI 的生产容量或资产一致性验收。
 */
@Timeout(30)
class MapTransportCapacityTest {
    private static final Context.Key<SocketAddress> PEER = Context.key("map-capacity-test-peer");
    private static final String DATASET = "998c5152-0369-42c3-a540-aa3a79844687";
    private static final String GENERATION = "5b46db68-cc1a-4d4b-904b-7b889206cb33";

    @Test void fullWatchPoolRejectsLocallyWhileQueryGameplayAndSessionStayIndependent() throws Exception {
        var peers = new PeerRecorder();
        var map = new MapService();
        var tokens = new TokenService("map-transport-test-secret-longer-than-32-bytes", Clock.systemUTC());
        Server server = server(2, peers, map, tokens);
        String target = "127.0.0.1:" + server.getPort();
        var settings = settings(8, 2, 2);
        var snapshots = new LinkedBlockingQueue<MapViewUpdate>();
        var failures = new LinkedBlockingQueue<Throwable>();
        var revoked = new LinkedBlockingQueue<String>();
        try (var client = new UpstreamClient(target, target, settings)) {
            var watches = new ArrayList<MapUpstreamClient.Watch>();
            for (int i = 0; i < 4; i++) {
                String id = "initial-" + i;
                watches.add(client.maps().watch(watch(id), "session", SessionTestService.KEY, snapshots::add, failures::add));
                assertSnapshot(id, snapshots);
            }
            assertEquals(4, client.maps().metrics().get("watchLimit"));
            assertEquals(4, client.maps().metrics().get("watchSlots"));
            assertThrows(RejectedExecutionException.class, () -> client.maps().watch(watch("over-budget"),
                    "session", SessionTestService.KEY, snapshots::add, failures::add));
            assertEquals(4, map.accepted.get(), "满池须在本地拒绝，不能把额外订阅塞入 HTTP/2 等待队列");

            var query = client.maps().query(MapQueryRequest.getDefaultInstance(), "session", SessionTestService.KEY);
            var play = client.play(PlayRequest.newBuilder().setMsgId(GameplayRoutes.GET_PLAYER)
                    .setRequestId("while-map-full").build(), "session", SessionTestService.KEY);
            String token = tokens.issue("101", Duration.ofMinutes(5));
            try (var session = client.sessions().open(token, "101", revoked::add).get(3, TimeUnit.SECONDS)) {
                assertEquals(7, query.get(3, TimeUnit.SECONDS).getSnapshot().getAppliedSeq());
                assertEquals("while-map-full", play.get(3, TimeUnit.SECONDS).getRequestId());
                assertTrue(session.valid(), "地图池占满时仍应能完成独立控制通道的 Prepare/Open");

                Set<SocketAddress> watchPeers = peers.forMethod(MapRpcGrpc.getWatchMethod().getFullMethodName());
                assertEquals(2, watchPeers.size(), "四条长流应分别占用两条真实连接，每条最多两个流");
                assertTrue(map.activeByPeer.values().stream().allMatch(count -> count.get() == 2));
                var distinct = new HashSet<>(watchPeers);
                for (String method : new String[] { MapRpcGrpc.getQueryMethod().getFullMethodName(),
                        GameRpcGrpc.getPlayMethod().getFullMethodName(), SessionRpcGrpc.getOpenMethod().getFullMethodName() }) {
                    Set<SocketAddress> lane = peers.forMethod(method);
                    assertEquals(1, lane.size());
                    assertTrue(distinct.add(lane.iterator().next()), "查询、玩法和会话各自使用独立于 Watch 池的连接");
                }
            }
            assertTrue(revoked.isEmpty());

            watches.getFirst().close();
            assertEquals("initial-0", map.cancelled.poll(3, TimeUnit.SECONDS));
            // 取消请求与 onClose 是异步边界；只有传输终态归还槽后，下一条流才可复用该连接。
            await(() -> ((Number) client.maps().metrics().get("watchSlots")).intValue() == 3);
            watches.add(client.maps().watch(watch("replacement"), "session", SessionTestService.KEY,
                    snapshots::add, failures::add));
            assertSnapshot("replacement", snapshots);
            watches.getFirst().close(); // 重复关闭旧流不能释放新流已取得的名额。
            assertEquals(4, client.maps().metrics().get("watchSlots"));
            assertThrows(RejectedExecutionException.class, () -> client.maps().watch(watch("still-full"),
                    "session", SessionTestService.KEY, snapshots::add, failures::add));
            assertTrue(failures.isEmpty(), "本地正常取消及满池拒绝不能误终止其他已建立订阅");
            client.close();
            await(() -> ((Number) client.maps().metrics().get("watchSlots")).intValue() == 0
                    && map.activeByPeer.values().stream().allMatch(count -> count.get() == 0));
        } finally {
            server.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
        }
    }

    @Test void pooledTransportKeepsMoreThan512RealWatchesOpen() throws Exception {
        var peers = new PeerRecorder();
        var map = new MapService();
        var tokens = new TokenService("map-transport-test-secret-longer-than-32-bytes", Clock.systemUTC());
        Server server = server(512, peers, map, tokens);
        String target = "127.0.0.1:" + server.getPort();
        var snapshots = new LinkedBlockingQueue<MapViewUpdate>();
        var failures = new LinkedBlockingQueue<Throwable>();
        try (var client = new UpstreamClient(target, target, settings(600, 3, 256))) {
            // 逐个确认首包后保持所有流开放，专测累计并发容量，不混入突发建连或 AOI 计算负载。
            for (int i = 0; i < 550; i++) {
                String id = "live-" + i;
                client.maps().watch(watch(id), "session", SessionTestService.KEY, snapshots::add, failures::add);
                assertSnapshot(id, snapshots);
            }
            assertEquals(550, map.accepted.get());
            assertEquals(550, client.maps().metrics().get("watchSlots"));
            assertEquals(550, client.maps().metrics().get("watches"));
            assertEquals(3, peers.forMethod(MapRpcGrpc.getWatchMethod().getFullMethodName()).size());
            assertTrue(map.activeByPeer.values().stream().allMatch(count -> count.get() <= 256));
            assertEquals(550, map.activeByPeer.values().stream().mapToInt(AtomicInteger::get).sum());
            assertTrue(failures.isEmpty(), "超过原单连接 512 上限的长流应持续有效");
            assertEquals(7, client.maps().query(MapQueryRequest.getDefaultInstance(), "session", SessionTestService.KEY)
                    .get(3, TimeUnit.SECONDS).getSnapshot().getAppliedSeq());
            // 先观察整个池的流终态，再关测试服务，避免强制停服掩盖客户端未回收的槽。
            client.close();
            await(() -> ((Number) client.maps().metrics().get("watchSlots")).intValue() == 0
                    && map.activeByPeer.values().stream().allMatch(count -> count.get() == 0));
        } finally {
            server.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
        }
    }

    private static GatewaySettings settings(int connections, int watchChannels, int perChannel) {
        return new GatewaySettings(connections, 6000, 60, 30, 10, 3000, 2, 4, watchChannels, perChannel);
    }

    private static MapWatchRequest watch(String id) {
        return MapWatchRequest.newBuilder().setSubscriptionId(id).build();
    }

    private static void assertSnapshot(String id, LinkedBlockingQueue<MapViewUpdate> snapshots) throws InterruptedException {
        var snapshot = snapshots.poll(3, TimeUnit.SECONDS);
        assertNotNull(snapshot, "订阅应在预算内收到首个完整快照：" + id);
        assertEquals(id, snapshot.getSubscriptionId());
        assertTrue(snapshot.getSnapshot());
        assertEquals(GENERATION, snapshot.getGeneration());
        assertEquals(7, snapshot.getThroughSeq());
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(5);
        assertTrue(condition.getAsBoolean(), "传输终态应在有界时间内归还名额");
    }

    private static Server server(int streamsPerConnection, PeerRecorder peers, MapService map, TokenService tokens)
            throws Exception {
        return RpcTransport.serverBuilder(new InetSocketAddress("127.0.0.1", 0))
                .maxConcurrentCallsPerConnection(streamsPerConnection).intercept(peers)
                .addService(map).addService(new SessionTestService(tokens))
                .addService(new GameRpcGrpc.GameRpcImplBase() {
                    @Override public void play(PlayRequest request, StreamObserver<PlayReply> output) {
                        output.onNext(PlayReply.newBuilder().setMsgId(request.getMsgId()).setRequestId(request.getRequestId())
                                .setStatus(200).setBody(ByteString.EMPTY).build());
                        output.onCompleted();
                    }
                }).build().start();
    }

    private static final class PeerRecorder implements ServerInterceptor {
        private final Map<String, Set<SocketAddress>> peers = new ConcurrentHashMap<>();
        @Override public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call, Metadata headers,
                                                                   ServerCallHandler<Q, R> next) {
            SocketAddress peer = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
            peers.computeIfAbsent(call.getMethodDescriptor().getFullMethodName(), ignored -> ConcurrentHashMap.newKeySet())
                    .add(peer);
            return Contexts.interceptCall(Context.current().withValue(PEER, peer), call, headers, next);
        }
        Set<SocketAddress> forMethod(String method) { return Set.copyOf(peers.getOrDefault(method, Set.of())); }
    }

    private static final class MapService extends MapRpcGrpc.MapRpcImplBase {
        private final AtomicInteger accepted = new AtomicInteger();
        private final Map<SocketAddress, AtomicInteger> activeByPeer = new ConcurrentHashMap<>();
        private final LinkedBlockingQueue<String> cancelled = new LinkedBlockingQueue<>();
        @Override public void watch(MapWatchRequest request, StreamObserver<MapViewUpdate> output) {
            var active = activeByPeer.computeIfAbsent(PEER.get(), ignored -> new AtomicInteger());
            active.incrementAndGet();
            accepted.incrementAndGet();
            ((ServerCallStreamObserver<MapViewUpdate>) output).setOnCancelHandler(() -> {
                active.decrementAndGet();
                cancelled.add(request.getSubscriptionId());
            });
            output.onNext(MapViewUpdate.newBuilder().setSubscriptionId(request.getSubscriptionId())
                    .setRealmId(1).setDatasetId(DATASET).setGeneration(GENERATION).setThroughSeq(7).setSnapshot(true).build());
            // 首包后保持流开放，避免短 RPC 已结束而错误掩盖 HTTP/2 的长期并发流上限。
        }
        @Override public void query(MapQueryRequest request, StreamObserver<MapQueryReply> output) {
            output.onNext(MapQueryReply.newBuilder().setSnapshot(MapViewSnapshot.newBuilder()
                    .setRealmId(1).setDatasetId(DATASET).setGeneration(GENERATION).setAppliedSeq(7)).build());
            output.onCompleted();
        }
    }
}
