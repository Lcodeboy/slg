package com.slg.game;

import com.google.protobuf.MessageLite;
import com.slg.aoi.AoiRuntime;
import com.slg.aoi.MapStateSource.*;
import com.slg.core.auth.TokenService;
import com.slg.gate.GatewayServer;
import com.slg.rpc.GatewayWire;
import com.slg.rpc.RpcMetadata;
import com.slg.rpc.RpcTransport;
import com.slg.rpc.WireFrame;
import com.slg.rpc.generated.WireRoutes;
import com.slg.rpc.v1.*;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.stub.MetadataUtils;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.*;

/**
 * 建角仍经真实应用/RPC；地图端口显式提供完整已提交决定，验证公开对象到 TCP 的原子可见性。
 * 业务扣兵/返兵和持久恢复由 MySQL March/Background IT 验证，本夹具不冒充数据库。
 */
@Timeout(30)
class WorldObjectsAoiFlowTest {
    private static final String FEED_TOKEN = "world-object-feed-only-test-credential-32";
    private static final MapViewport ALL = MapViewport.newBuilder().setMinX(0).setMinY(0).setMaxX(1199).setMaxY(1199).build();

    @Test void citiesAndStaticMinesShareSnapshotsAndAtomicOccupationUpdatesAcrossRebuild() throws Exception {
        UUID volumeId = UUID.randomUUID();
        var source = new MapSourceFixture(1, volumeId);
        List<MapObjectView> finalObjects;
        try (var game = new Harness(source);
             var observer = game.connect("102"); var actor = game.connect("101")) {
            observer.call(WireRoutes.CREATE_PLAYER, CreatePlayerReq.newBuilder().setName("Observer").build());
            source.advance(List.of(), List.of(), List.of(new City("1_102", 1, 0, 0, "Observer")));
            game.await(1);
            observer.call(WireRoutes.MAP_WATCH, watch("world-first", ALL));
            var initial = observer.update(1);
            assertTrue(initial.getSnapshot());
            assertEquals(4, initial.getObjectUpsertsCount(), "三座未采过的矿与观察者城市均必须出现");
            assertEquals(3, initial.getObjectUpsertsList().stream().filter(MapObjectView::hasResourceSite).count());
            initial.getObjectUpsertsList().stream().filter(MapObjectView::hasResourceSite).forEach(site -> {
                assertEquals(0, site.getVersion()); assertTrue(site.getResourceSite().getOccupantPlayerId().isEmpty());
            });

            actor.call(WireRoutes.CREATE_PLAYER, CreatePlayerReq.newBuilder().setName("Alice").build());
            source.advance(List.of(), List.of(), List.of(new City("1_101", 1, 0, 0, "Alice")));
            var created = observer.update(2);
            var city = object(created, "city:1_101");
            assertEquals("Alice", city.getCity().getName());
            assertEquals("1_101", city.getCity().getPlayerId());
            assertEquals(0, city.getX()); assertEquals(0, city.getY());
            assertEquals(1, city.getVersion());
            assertTrue(created.getRemovedObjectIdsList().isEmpty());

            var sent = new Route("world-march", 1, Direction.OUTBOUND, 0, 0, 150, 150, 1000, 4000);
            source.advance(List.of(sent), List.of(), List.of());
            var outgoing = observer.update(3);
            assertEquals(sent.marchId(), outgoing.getUpserts(0).getMarchId());
            assertTrue(outgoing.getObjectUpsertsList().isEmpty(), "资产与行军变化不能伪增 profile 城市版本");
            source.advance(List.of(), List.of(sent.marchId()), List.of(new ResourceSite(150, 150, 1, "WOOD", "1_101")));
            var occupied = observer.update(4);
            assertEquals(List.of(sent.marchId()), occupied.getRemovedIdsList());
            var mine = object(occupied, "resource:150-150");
            assertEquals(1, mine.getVersion());
            assertEquals("1_101", mine.getResourceSite().getOccupantPlayerId());
            assertEquals("WOOD", mine.getResourceSite().getResourceType());

            // 可选远程供数必须包含与本地 AOI 相同的原子事实，不能仍只暴露路线删除。
            var feed = game.feed().changes(MapChangesReq.newBuilder().setRealmId(1).setDatasetId(volumeId.toString())
                    .setAfterSeq(3).setLimit(1).build());
            assertEquals(4, feed.getThroughSeq());
            assertEquals(1, feed.getChangesCount()); assertTrue(feed.getChanges(0).getRemoved());
            assertEquals(1, feed.getObjectChangesCount());
            assertEquals(4, feed.getObjectChanges(0).getSequence());
            assertEquals(mine, feed.getObjectChanges(0).getUpsert());

            var small = MapViewport.newBuilder().setMinX(150).setMinY(150).setMaxX(150).setMaxY(150).build();
            var query = MapQueryReply.parseFrom(observer.call(WireRoutes.MAP_QUERY,
                    MapQueryRequest.newBuilder().setViewport(small).build()).body()).getSnapshot();
            assertEquals(List.of(mine), query.getObjectsList(), "边界点命中，其他矿和原点城市不混入局部视野");
            assertEquals(0, query.getRoutesCount());

            // 采集中召回释放矿点并生成返程；矿点不能被删掉，也不能等返城后才变为空闲。
            source.advance(List.of(new Route(sent.marchId(), 3, Direction.RETURNING, 150, 150, 0, 0, 5000, 8000)),
                    List.of(), List.of(new ResourceSite(150, 150, 2, "WOOD", "")));
            var released = observer.update(5);
            assertEquals(MarchState.MARCH_STATE_RETURNING, released.getUpserts(0).getState());
            var free = object(released, "resource:150-150");
            assertEquals(2, free.getVersion()); assertTrue(free.getResourceSite().getOccupantPlayerId().isEmpty());
            assertTrue(released.getRemovedObjectIdsList().isEmpty());
            source.advance(List.of(), List.of(sent.marchId()), List.of());
            var completed = observer.update(6);
            assertEquals(List.of(sent.marchId()), completed.getRemovedIdsList());
            assertTrue(completed.getObjectUpsertsList().isEmpty());

            observer.call(WireRoutes.MAP_CANCEL, MapWatchCancel.newBuilder().setSubscriptionId("world-first").build());
            observer.call(WireRoutes.MAP_WATCH, watch("world-rebuilt", ALL));
            var fresh = observer.update(6);
            assertTrue(fresh.getSnapshot()); assertEquals("world-rebuilt", fresh.getSubscriptionId());
            assertEquals(5, fresh.getObjectUpsertsCount()); assertEquals(0, fresh.getUpsertsCount());
            assertEquals(free, object(fresh, "resource:150-150"));
            assertEquals(1, object(fresh, "city:1_101").getVersion());
            finalObjects = fresh.getObjectUpsertsList();
            var state = GetPlayerRsp.parseFrom(actor.call(WireRoutes.GET_PLAYER, GetPlayerReq.getDefaultInstance()).body()).getPlayer();
            assertEquals(3000, state.getTroopsOrThrow("INFANTRY")); assertEquals(0, state.getActiveMarchIdsCount());

            var remoteObjects = new ArrayList<MapObjectView>();
            var chunks = game.feed().snapshot(MapSnapshotReq.newBuilder().setRealmId(1).setDatasetId(volumeId.toString()).build());
            boolean complete = false;
            while (chunks.hasNext()) {
                var chunk = chunks.next(); assertFalse(complete); assertEquals(6, chunk.getHighSeq());
                remoteObjects.addAll(chunk.getObjectsList()); complete = chunk.getComplete();
            }
            assertTrue(complete);
            assertEquals(sorted(finalObjects), sorted(remoteObjects), "本地与远程快照的静态覆盖和动态字段必须一致");
        }
        try (var reopened = new Harness(source); var observer = reopened.connect("102")) {
            reopened.await(6);
            var restored = MapQueryReply.parseFrom(observer.call(WireRoutes.MAP_QUERY,
                    MapQueryRequest.newBuilder().setViewport(ALL).build()).body()).getSnapshot();
            assertEquals(sorted(finalObjects), sorted(restored.getObjectsList()));
            assertEquals(0, restored.getRoutesCount(), "重建不得把已完成的路线或旧矿主复活");
        }
    }

    private static List<MapObjectView> sorted(List<MapObjectView> list) {
        return list.stream().sorted(java.util.Comparator.comparing(MapObjectView::getObjectId)).toList();
    }
    private static MapObjectView object(MapViewUpdate update, String id) {
        return update.getObjectUpsertsList().stream().filter(value -> value.getObjectId().equals(id)).findFirst().orElseThrow();
    }
    private static MapWatchRequest watch(String id, MapViewport view) {
        return MapWatchRequest.newBuilder().setSubscriptionId(id).setViewport(view).build();
    }
    private static InetSocketAddress local() { return new InetSocketAddress("127.0.0.1", 0); }

    private static final class Harness implements AutoCloseable {
        final PlayerApplicationFixture fixture = new PlayerApplicationFixture();
        final TokenService tokens = fixture.tokens;
        final SessionDirectory sessions = fixture.sessions;
        final AoiRuntime aoi;
        final AoiRpcService maps;
        final MapFeedService mapFeed;
        final Server server;
        final ManagedChannel channel;
        final GatewayServer gateway;
        Harness(MapSourceFixture source) throws Exception {
            aoi = new AoiRuntime(source, AoiRuntime.Settings.defaults());
            maps = new AoiRpcService(aoi, tokens, () -> true, source::allows, sessions);
            mapFeed = new MapFeedService(source, FEED_TOKEN, () -> true);
            aoi.start();
            server = RpcTransport.serverBuilder(local()).addService(maps).addService(mapFeed)
                    .addService(fixture.gameplay)
                    .addService(fixture.controls).build().start();
            String target = "127.0.0.1:" + server.getPort();
            channel = RpcTransport.channel(target);
            gateway = new GatewayServer(target, target, tokens); gateway.start(local(), local());
        }
        Tcp connect(String account) throws Exception { return new Tcp(gateway.tcpPort(), tokens.issue(account, Duration.ofHours(1))); }
        void await(long sequence) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!"READY".equals(aoi.metrics().get("state")) || (long) aoi.metrics().get("appliedSeq") < sequence) {
                assertTrue(System.nanoTime() < until, () -> aoi.metrics().toString()); Thread.sleep(5);
            }
        }
        MapFeedRpcGrpc.MapFeedRpcBlockingStub feed() {
            return MapFeedRpcGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(RpcMetadata.headers(FEED_TOKEN, null)));
        }
        public void close() throws Exception {
            gateway.close(); channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            maps.close(); mapFeed.close(); aoi.close(); fixture.close();
        }
    }
    /** 与生产客户端相同，一条 socket 只有一个读入口，等待响应时暂存先到的地图推送。 */
    private static final class Tcp implements AutoCloseable {
        final Socket socket;
        final DataInputStream input;
        final DataOutputStream output;
        final ArrayDeque<MapViewUpdate> updates = new ArrayDeque<>();
        Tcp(int port, String token) throws Exception {
            socket = new Socket("127.0.0.1", port); socket.setSoTimeout(5000);
            input = new DataInputStream(socket.getInputStream()); output = new DataOutputStream(socket.getOutputStream());
            call(WireRoutes.AUTH, GatewayAuth.newBuilder().setToken(token).build());
        }
        WireFrame read() throws Exception {
            int size = input.readInt();
            assertTrue(size >= GatewayWire.REMAINDER_HEADER_BYTES && size <= GatewayWire.MAX_SERVER_FRAME_BYTES);
            var bytes = input.readNBytes(size); assertEquals(size, bytes.length); return GatewayWire.decode(bytes);
        }
        WireFrame call(int id, MessageLite message) throws Exception {
            UUID request = UUID.randomUUID();
            output.write(GatewayWire.encode(WireFrame.request(id, request, message))); output.flush();
            for (;;) {
                var frame = read();
                if (frame.kind() == WireFrame.PUSH && frame.msgId() == WireRoutes.MAP_VIEW_UPDATE) {
                    assertTrue(updates.size() < 16); updates.add(MapViewUpdate.parseFrom(frame.body())); continue;
                }
                assertEquals(WireFrame.RESPONSE, frame.kind()); assertEquals(request, frame.requestId());
                assertEquals(id, frame.msgId()); assertEquals(0, frame.status(), frame.toString()); return frame;
            }
        }
        MapViewUpdate update(long sequence) throws Exception {
            for (int attempts = 0; attempts < 16; attempts++) {
                MapViewUpdate update;
                if (!updates.isEmpty()) update = updates.removeFirst();
                else {
                    var frame = read(); assertEquals(WireRoutes.MAP_VIEW_UPDATE, frame.msgId());
                    update = MapViewUpdate.parseFrom(frame.body());
                }
                assertFalse(update.hasError(), update.toString());
                if (update.getThroughSeq() < sequence) continue;
                assertEquals(sequence, update.getThroughSeq()); return update;
            }
            throw new AssertionError("Expected map update at " + sequence);
        }
        public void close() throws Exception { socket.close(); }
    }
}
