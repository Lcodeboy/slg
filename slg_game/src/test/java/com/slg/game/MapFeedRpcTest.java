package com.slg.game;

import com.slg.core.auth.TokenService;
import com.slg.aoi.MapSnapshotSource;
import com.slg.aoi.MapStateSource;
import com.slg.aoi.MapStateSource.Route;
import com.slg.aoi.MapStateSource.Direction;
import com.slg.aoi.MapStateSource.City;
import com.slg.aoi.MapStateSource.ResourceSite;
import com.slg.rpc.RpcMetadata;
import com.slg.rpc.RpcTransport;
import com.slg.rpc.v1.MapChangesPage;
import com.slg.rpc.v1.MapChangesReq;
import com.slg.rpc.v1.MapFeedRpcGrpc;
import com.slg.rpc.v1.MapObjectView;
import com.slg.rpc.v1.MapRoute;
import com.slg.rpc.v1.MapRouteChange;
import com.slg.rpc.v1.MapSnapshotChunk;
import com.slg.rpc.v1.MapSnapshotReq;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.MetadataUtils;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import static com.slg.rpc.v1.MarchState.MARCH_STATE_OUTBOUND;
import static com.slg.rpc.v1.MarchState.MARCH_STATE_RETURNING;
import static org.junit.jupiter.api.Assertions.*;

/**
 * 真实 gRPC 上验证通用地图供数契约、混合分块及取消背压；有界内存源只模拟已提交决定。
 * 本类不证明数据库事务或进程持久恢复，后者由 MySqlMarch/BackgroundApplicationIT 验证。
 */
class MapFeedRpcTest {
    private static final int REALM = 17;
    private static final String OWNER = "101";
    private static final String SECRET = "test-only-32-byte-map-consumer-credential";
    private static final Instant DEPARTURE = Instant.parse("2026-01-01T10:00:00Z");
    private final UUID datasetId = UUID.randomUUID();
    private final MapSourceFixture source = new MapSourceFixture(REALM, datasetId);

    @Test void snapshotAndResumeFollowWholeDecisionsAcrossConsumerReconnect() throws Exception {
        String marchId = "march-reconnect";
        MapRouteChange beforeReconnect;
        try (var feed = new Endpoint(source)) {
            var client = feed.client(SECRET);
            assertTrue(snapshot(client, 0).isEmpty());
            source.advance(List.of(), List.of(), List.of(new City("17_101", 1, 0, 0, "Alice")));
            long departure = DEPARTURE.toEpochMilli();
            source.advance(List.of(new Route(marchId, 1, Direction.OUTBOUND, 0, 0,
                    150, 150, departure, departure + 3000)), List.of(), List.of());

            // J1 只有城市变化，J2 才有路线；through 必须涵盖完整决定而非最后可见路线。
            var city = client.changes(changes(0, 1));
            assertEquals(0, city.getChangesCount());
            assertEquals("city:17_101", city.getObjectChanges(0).getObjectId());
            assertEquals(1, city.getThroughSeq());
            assertEquals(2, city.getHighSeq());
            var departed = client.changes(changes(1, 1));
            assertEquals(2, departed.getThroughSeq());
            assertEquals(1, departed.getChangesCount());
            var outbound = departed.getChanges(0).getUpsert();
            assertEquals(MARCH_STATE_OUTBOUND, outbound.getState());
            assertEquals(150, outbound.getTargetX());
            assertEquals(departure, outbound.getDepartAt());
            assertEquals(List.of(outbound), snapshot(client, 2));

            // 源已确认的亚格坐标必须无损经过 wire；这里不重新计算召回规则。
            source.advance(List.of(new Route(marchId, 2, Direction.RETURNING, 37.5, 37.5,
                    0, 0, departure + 750, departure + 1500)), List.of(), List.of());
            var recalled = client.changes(changes(2, 8));
            beforeReconnect = recalled.getChanges(0);
            assertEquals(3, beforeReconnect.getSequence());
            assertEquals(2, beforeReconnect.getVersion());
            var returning = beforeReconnect.getUpsert();
            assertEquals(MARCH_STATE_RETURNING, returning.getState());
            assertEquals(37.5, returning.getFromX());
            assertEquals(37.5, returning.getFromY());
            assertEquals(0, returning.getTargetX());
            assertEquals(List.of(returning), snapshot(client, 3));
        }
        // 消费者仍停在 J2，重建传输后补读已保留决定。不会把内存夹具冒充数据库重启恢复。
        source.advance(List.of(), List.of(marchId), List.of());
        try (var feed = new Endpoint(source)) {
            var client = feed.client(SECRET);
            var replay = client.changes(changes(2, 8));
            assertEquals(REALM, replay.getRealmId());
            assertEquals(datasetId.toString(), replay.getDatasetId());
            assertEquals(0, replay.getPrunedThrough());
            assertEquals(4, replay.getHighSeq());
            assertEquals(4, replay.getThroughSeq());
            assertEquals(2, replay.getChangesCount());
            assertEquals(beforeReconnect, replay.getChanges(0));
            var removed = replay.getChanges(1);
            assertEquals(4, removed.getSequence());
            assertEquals(marchId, removed.getMarchId());
            assertEquals(3, removed.getVersion());
            assertTrue(removed.getRemoved());
            assertTrue(snapshot(client, 4).isEmpty());
            var caughtUp = client.changes(changes(4, 8));
            assertEquals(4, caughtUp.getThroughSeq());
            assertEquals(0, caughtUp.getChangesCount());
        }
    }

    @Test void rejectsMissingCredentialsWrongSourceAndInvalidCursorWithoutAdvancingTheSource() throws Exception {
        try (var feed = new Endpoint(source)) {
            var request = snapshotRequest();
            assertError(Status.Code.UNAUTHENTICATED, () -> feed.client(null).snapshot(request).hasNext());
            // 合法的玩家会话也不能读取全区路线，因为这个入口的权限属于 AOI 服务。
            String session = new TokenService("test-only-player-signing-secret-key", Clock.systemUTC())
                    .issue(OWNER, Duration.ofMinutes(1));
            assertError(Status.Code.UNAUTHENTICATED, () -> feed.client(session).changes(changes(0, 1)));
            var client = feed.client(SECRET);
            assertError(Status.Code.FAILED_PRECONDITION, () -> client.snapshot(request.toBuilder()
                    .setDatasetId(UUID.randomUUID().toString()).build()).hasNext());
            assertError(Status.Code.FAILED_PRECONDITION, () -> client.changes(changes(0, 1).toBuilder()
                    .setRealmId(REALM + 1).build()));
            assertError(Status.Code.FAILED_PRECONDITION, () -> client.changes(changes(1, 1)));
            assertError(Status.Code.INVALID_ARGUMENT, () -> client.changes(changes(0, 129)));
            assertError(Status.Code.INVALID_ARGUMENT, () -> client.changes(changes(0, 0)));
            assertEquals(0, source.high());
        }
    }

    @Test void corruptSourceFailsSnapshotAndDeltaWithoutReportingCompletion() throws Exception {
        var broken = new MapSnapshotSource() {
            @Override public Identity identity() { return source.identity(); }
            @Override public long snapshot(Check check, SnapshotReceiver receiver) throws Exception {
                receiver.begin(1);
                throw new MapStateSource.BadData("Cannot decode committed route");
            }
            @Override public Page changes(long after, int limit) throws Exception {
                throw new MapStateSource.BadData("Cannot decode committed decision");
            }
        };
        try (var feed = new Endpoint(broken)) {
            var client = feed.client(SECRET);
            assertError(Status.Code.DATA_LOSS, () -> client.snapshot(snapshotRequest()).hasNext());
            assertError(Status.Code.DATA_LOSS, () -> client.changes(changes(0, 1)));
        }
    }

    @Test void enablingTheFeedRequiresIndependentCredentialsAndRemoteListenersRequireMutualTls() {
        var loopback = new InetSocketAddress("127.0.0.1", 0);
        var allInterfaces = new InetSocketAddress("0.0.0.0", 0);
        assertNull(MapFeedService.configured(source, () -> true, loopback, Map.of()));
        assertThrows(IllegalArgumentException.class, () -> MapFeedService.configured(source,
                () -> true, loopback, Map.of("SLG_MAP_FEED_TOKEN", "short")));
        assertThrows(IllegalArgumentException.class, () -> MapFeedService.configured(source,
                () -> true, loopback, Map.of("SLG_MAP_FEED_TOKEN", SECRET, "SLG_TOKEN_SECRET", SECRET)));
        assertThrows(IllegalArgumentException.class, () -> MapFeedService.configured(source,
                () -> true, allInterfaces, Map.of("SLG_MAP_FEED_TOKEN", SECRET)));
    }

    @Test void stalledSnapshotHasOneSlotAndCancellationAllowsACompleteRebuild() throws Exception {
        try (var feed = new Endpoint(source)) {
            // 有界夹具批量模拟已经确认的公开路线，只制造网络背压，不绕到玩法/SQL路径。
            int batches = 48, routesPerBatch = 128;
            for (int page = 0; page < batches; page++) {
                var rows = new ArrayList<Route>();
                for (int i = 0; i < routesPerBatch; i++)
                    rows.add(route("synthetic-backpressure-march-" + page + "-" + i));
                source.advance(rows, List.of(), List.of());
            }
            // 固定 HTTP/2 窗口并只读一块，避免自动扩窗把完整快照先存进本机传输缓冲区。
            var slowChannel = NettyChannelBuilder.forAddress("127.0.0.1", feed.server.getPort())
                    .usePlaintext().flowControlWindow(65_535).build();
            var slow = new SlowSnapshot();
            try {
                MapFeedRpcGrpc.newStub(slowChannel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(
                        headers(SECRET))).withDeadlineAfter(30, TimeUnit.SECONDS).snapshot(snapshotRequest(), slow);
                assertFalse(slow.first.get(5, TimeUnit.SECONDS).getComplete());
                var client = feed.client(SECRET);
                assertError(Status.Code.RESOURCE_EXHAUSTED, () -> client.snapshot(snapshotRequest()).hasNext());

                source.advance(List.of(), List.of(), List.of());
                var progressed = client.changes(changes(batches, 1));
                assertEquals(batches + 1, progressed.getThroughSeq(), "慢读快照期间写入与增量仍可推进");
                assertEquals(0, progressed.getChangesCount());
                slow.call.cancel("test consumer disconnected", null);
                assertEquals(Status.Code.CANCELLED, slow.finished.get(5, TimeUnit.SECONDS));

                // 收到本地取消通知和服务端收到 RST_STREAM 是两个时刻；只对名额尚未释放做有界重试。
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (true) {
                    try {
                        assertEquals(batches * routesPerBatch, snapshot(feed.client(SECRET), batches + 1).size());
                        break;
                    } catch (StatusRuntimeException busy) {
                        if (busy.getStatus().getCode() != Status.Code.RESOURCE_EXHAUSTED
                                || System.nanoTime() >= deadline) throw busy;
                        Thread.sleep(10);
                    }
                }
            } finally {
                slowChannel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test void mixedSnapshotCountsRoutesAndObjectsTogetherAndCompletesOnlyAfterTheFinalChunk() throws Exception {
        try (var feed = new Endpoint(source)) {
            source.advance(List.of(), List.of(), List.of(new City("17_101", 1, 0, 0, "MixedCity")));
            source.advance(List.of(), List.of(), List.of(new ResourceSite(150, 150, 1, "WOOD", "17_101")));
            // 126 路线+1城市+3矿点=130，必须共用128条块预算；占矿后像覆盖静态矿。
            var rows = new ArrayList<Route>();
            var expectedRoutes = new HashSet<String>();
            for (int i = 0; i < 126; i++) {
                String id = "mixed-snapshot-march-" + i;
                expectedRoutes.add(id);
                rows.add(route(id));
            }
            source.advance(rows, List.of(), List.of());
            long high = source.high();
            assertEquals(3, high);

            var frames = feed.client(SECRET).snapshot(snapshotRequest());
            var chunks = new ArrayList<MapSnapshotChunk>();
            var receivedRoutes = new HashSet<String>();
            var objects = new ArrayList<MapObjectView>();
            var objectIds = new HashSet<String>();
            boolean complete = false;
            while (frames.hasNext()) {
                var chunk = frames.next();
                assertFalse(complete, "完成标记后不得再有快照块");
                assertEquals(REALM, chunk.getRealmId());
                assertEquals(datasetId.toString(), chunk.getDatasetId());
                assertEquals(high, chunk.getHighSeq(), "路线与静态/动态点对象必须固定在同一个 H");
                assertTrue(chunk.getRoutesCount() + chunk.getObjectsCount() <= 128);
                assertTrue(chunk.getSerializedSize() <= RpcTransport.MAX_RESPONSE_BYTES);
                for (var route : chunk.getRoutesList())
                    assertTrue(receivedRoutes.add(route.getMarchId()), "跨块路线不能重复");
                for (var object : chunk.getObjectsList()) {
                    assertTrue(objectIds.add(object.getObjectId()), "占矿后像必须覆盖默认矿，不能输出两个相同 ID");
                    objects.add(object);
                }
                chunks.add(chunk);
                complete = chunk.getComplete();
                if (chunks.size() == 1) {
                    // 源已推进时，该快照剩余块及完成标记仍必须沿用固定 H。
                    source.advance(List.of(), List.of(), List.of());
                }
            }
            assertTrue(complete, "正常结束必须收到最后一个 complete 块");
            assertEquals(2, chunks.size());
            assertEquals(128, chunks.getFirst().getRoutesCount() + chunks.getFirst().getObjectsCount());
            assertFalse(chunks.getFirst().getComplete());
            assertTrue(chunks.getFirst().getRoutesCount() > 0 && chunks.getFirst().getObjectsCount() > 0,
                    "该用例必须实际生成路线与点对象混合的一块");
            assertEquals(2, chunks.getLast().getRoutesCount() + chunks.getLast().getObjectsCount());
            assertTrue(chunks.getLast().getComplete());
            assertEquals(high + 1, source.high());
            assertEquals(expectedRoutes, receivedRoutes);
            assertEquals(4, objects.size());
            var city = objects.stream().filter(MapObjectView::hasCity).findFirst().orElseThrow();
            assertEquals("city:17_101", city.getObjectId());
            assertEquals("MixedCity", city.getCity().getName());
            var mines = objects.stream().filter(MapObjectView::hasResourceSite).toList();
            assertEquals(3, mines.size());
            for (var mine : mines) {
                assertEquals("WOOD", mine.getResourceSite().getResourceType());
                if (mine.getObjectId().equals("resource:150-150")) {
                    assertEquals(1, mine.getVersion());
                    assertEquals("17_101", mine.getResourceSite().getOccupantPlayerId());
                } else {
                    assertEquals(0, mine.getVersion());
                    assertTrue(mine.getResourceSite().getOccupantPlayerId().isEmpty());
                }
            }
            assertEquals(java.util.Set.of("city:17_101", "resource:150-150", "resource:650-650",
                    "resource:1150-1150"), objectIds);
        }
    }

    private static final class SlowSnapshot implements ClientResponseObserver<MapSnapshotReq, MapSnapshotChunk> {
        private final CompletableFuture<MapSnapshotChunk> first = new CompletableFuture<>();
        private final CompletableFuture<Status.Code> finished = new CompletableFuture<>();
        private ClientCallStreamObserver<MapSnapshotReq> call;

        @Override public void beforeStart(ClientCallStreamObserver<MapSnapshotReq> requestStream) {
            call = requestStream;
            call.disableAutoRequestWithInitial(1);
        }
        @Override public void onNext(MapSnapshotChunk chunk) { first.complete(chunk); }
        @Override public void onError(Throwable failure) {
            first.completeExceptionally(failure);
            finished.complete(Status.fromThrowable(failure).getCode());
        }
        @Override public void onCompleted() { finished.complete(Status.Code.OK); }
    }

    private static Route route(String id) {
        return new Route(id, 1, Direction.OUTBOUND, 0, 0, 1150, 1150, 10_000, 27_000);
    }

    private MapSnapshotReq snapshotRequest() {
        return MapSnapshotReq.newBuilder().setRealmId(REALM).setDatasetId(datasetId.toString()).build();
    }

    private MapChangesReq changes(long after, int limit) {
        return MapChangesReq.newBuilder().setRealmId(REALM).setDatasetId(datasetId.toString())
                .setAfterSeq(after).setLimit(limit).build();
    }

    /** 模拟接收端验证终止块；只有读完同一水位的全部块才能用新地图替换旧索引。 */
    private List<MapRoute> snapshot(MapFeedRpcGrpc.MapFeedRpcBlockingStub client, long expectedHigh) {
        var received = new ArrayList<MapRoute>();
        var frames = client.snapshot(snapshotRequest());
        boolean complete = false;
        while (frames.hasNext()) {
            var chunk = frames.next();
            assertFalse(complete, "终止块必须是最后一块");
            assertEquals(REALM, chunk.getRealmId());
            assertEquals(datasetId.toString(), chunk.getDatasetId());
            assertEquals(expectedHigh, chunk.getHighSeq());
            received.addAll(chunk.getRoutesList());
            complete = chunk.getComplete();
        }
        assertTrue(complete, "空快照也有明确结束标记和水位");
        return received;
    }

    private static void assertError(Status.Code code, Executable call) {
        assertEquals(code, assertThrows(StatusRuntimeException.class, call).getStatus().getCode());
    }

    private static Metadata headers(String secret) {
        var headers = new Metadata();
        headers.put(RpcMetadata.AUTHORIZATION, "Bearer " + secret);
        return headers;
    }

    private final class Endpoint implements AutoCloseable {
        private final MapFeedService feed;
        private final Server server;
        private final ManagedChannel channel;

        Endpoint(MapSnapshotSource source) throws IOException {
            feed = new MapFeedService(source, SECRET, () -> true);
            server = RpcTransport.serverBuilder(new InetSocketAddress("127.0.0.1", 0)).addService(feed).build().start();
            channel = RpcTransport.channel("127.0.0.1:" + server.getPort());
        }

        MapFeedRpcGrpc.MapFeedRpcBlockingStub client(String secret) {
            var stub = MapFeedRpcGrpc.newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS);
            if (secret == null) return stub;
            return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers(secret)));
        }

        @Override public void close() throws InterruptedException {
            try {
                channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
                server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            } finally {
                feed.close();
            }
        }
    }
}
