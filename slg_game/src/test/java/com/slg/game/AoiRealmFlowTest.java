package com.slg.game;

import com.slg.aoi.AoiRuntime;
import com.slg.aoi.MapStateSource.*;
import com.slg.core.auth.TokenService;
import com.slg.core.world.Viewport;
import com.slg.rpc.RpcMetadata;
import com.slg.rpc.RpcTransport;
import com.slg.rpc.v1.*;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** 公开地图端口→本地 AOI→真实 gRPC；业务与数据库恢复另由 MySQL IT 验证。 */
class AoiRealmFlowTest {
    private static final int REALM = 17;
    private static final String OBSERVER = "102";
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final Viewport CENTER = new Viewport(550, 550, 650, 650);
    private static final TokenService TOKENS = new TokenService("aoi-integration-test-secret-32-bytes", Clock.fixed(NOW, ZoneOffset.UTC));

    @Test void centralObserverSeesWholeLongLineWithBothEndpointsOutsideViewport() throws Exception {
        var id = UUID.randomUUID(); var source = new MapSourceFixture(REALM, id);
        source.advance(List.of(route(1, Direction.OUTBOUND)), List.of(), List.of(new City("17_102", 1, 0, 0, "B")));
        try (var endpoint = new Endpoint(source)) {
            endpoint.awaitSequence(1);
            var snapshot = endpoint.client(OBSERVER).query(query()).getSnapshot();
            assertEquals(REALM, snapshot.getRealmId()); assertEquals(id.toString(), snapshot.getDatasetId());
            assertEquals(1, snapshot.getAppliedSeq()); assertEquals(1, snapshot.getRoutesCount());
            assertEquals(0, snapshot.getRoutes(0).getFromX()); assertEquals(1199, snapshot.getRoutes(0).getTargetX());
        }
    }

    @Test void returningAndRemovalReachWatchAndRebuildDoesNotResurrectTheLine() throws Exception {
        var source = new MapSourceFixture(REALM, UUID.randomUUID());
        source.advance(List.of(route(1, Direction.OUTBOUND)), List.of(), List.of(new City("17_102", 1, 0, 0, "B")));
        String generation;
        try (var endpoint = new Endpoint(source)) {
            endpoint.awaitSequence(1); var updates = new Updates();
            MapRpcGrpc.newStub(endpoint.channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(endpoint.headers(OBSERVER)))
                    .watch(MapWatchRequest.newBuilder().setSubscriptionId("central-view").setViewport(wireViewport()).build(), updates);
            var initial = updates.next(); assertTrue(initial.getSnapshot()); generation = initial.getGeneration();
            source.advance(List.of(route(2, Direction.RETURNING)), List.of(), List.of());
            var changed = updates.through(2);
            assertFalse(changed.getSnapshot()); assertEquals(generation, changed.getGeneration());
            assertEquals(1, changed.getFromSeq()); assertEquals(MarchState.MARCH_STATE_RETURNING, changed.getUpserts(0).getState());
            endpoint.awaitSequence(2);
            assertEquals(changed.getUpsertsList(), endpoint.client(OBSERVER).query(query()).getSnapshot().getRoutesList());
            source.advance(List.of(), List.of("long-route"), List.of());
            assertEquals(List.of("long-route"), updates.through(3).getRemovedIdsList()); endpoint.awaitSequence(3);
        }
        try (var endpoint = new Endpoint(source)) {
            endpoint.awaitSequence(3); var snapshot = endpoint.client(OBSERVER).query(query()).getSnapshot();
            assertNotEquals(generation, snapshot.getGeneration()); assertEquals(0, snapshot.getRoutesCount());
            assertEquals(3, snapshot.getAppliedSeq());
        }
    }

    @Test void mapEntryRequiresSessionAndRoleAndRejectsInvalidViewport() throws Exception {
        var source = new MapSourceFixture(REALM, UUID.randomUUID());
        try (var endpoint = new Endpoint(source)) {
            endpoint.awaitSequence(0);
            assertEquals(Status.Code.UNAUTHENTICATED, assertThrows(StatusRuntimeException.class,
                    () -> MapRpcGrpc.newBlockingStub(endpoint.channel).query(query())).getStatus().getCode());
            assertEquals(Status.Code.PERMISSION_DENIED, assertThrows(StatusRuntimeException.class,
                    () -> endpoint.client(OBSERVER).query(query())).getStatus().getCode());
            source.advance(List.of(), List.of(), List.of(new City("17_102", 1, 0, 0, "B")));
            assertEquals(Status.Code.INVALID_ARGUMENT, assertThrows(StatusRuntimeException.class,
                    () -> endpoint.client(OBSERVER).query(query().toBuilder()
                            .setViewport(wireViewport().toBuilder().setMinX(Double.NaN)).build())).getStatus().getCode());
        }
    }
    private static Route route(long version, Direction direction) {
        return direction == Direction.OUTBOUND ? new Route("long-route", version, direction, 0, 0, 1199, 1199, 1000, 100000)
                : new Route("long-route", version, direction, 900, 900, 0, 0, 2000, 100000);
    }
    private static MapViewport wireViewport() {
        return MapViewport.newBuilder().setMinX(550).setMinY(550).setMaxX(650).setMaxY(650).build();
    }
    private static MapQueryRequest query() {
        return MapQueryRequest.newBuilder().setViewport(wireViewport()).build();
    }
    private static final class Endpoint implements AutoCloseable {
        final AoiRuntime aoi;
        final SessionDirectory sessions = new SessionDirectory(Clock.fixed(NOW, ZoneOffset.UTC), 32);
        final java.util.Map<String, Metadata> bindings = new java.util.HashMap<>();
        final AoiRpcService rpc;
        final Server server;
        final ManagedChannel channel;
        Endpoint(MapSourceFixture source) throws Exception {
            aoi = new AoiRuntime(source, AoiRuntime.Settings.defaults());
            rpc = new AoiRpcService(aoi, TOKENS, () -> true, source::allows, sessions);
            server = RpcTransport.serverBuilder(new InetSocketAddress("127.0.0.1", 0)).addService(rpc).build().start();
            channel = RpcTransport.channel("127.0.0.1:" + server.getPort());
            aoi.start();
        }
        Metadata headers(String account) {
            return bindings.computeIfAbsent(account, key -> {
                var context = SessionTestSupport.context(sessions, TOKENS, key);
                return RpcMetadata.headers(RpcMetadata.TOKEN.get(context), RpcMetadata.SESSION.get(context));
            });
        }
        MapRpcGrpc.MapRpcBlockingStub client(String account) {
            return MapRpcGrpc.newBlockingStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers(account)))
                    .withDeadlineAfter(5, TimeUnit.SECONDS);
        }
        void awaitSequence(long sequence) throws Exception {
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
            Throwable last = null;
            while (System.nanoTime() < end) {
                try { if (aoi.query(CENTER).get(1, TimeUnit.SECONDS).appliedSeq() >= sequence) return; }
                catch (java.util.concurrent.ExecutionException notReady) { last = notReady.getCause(); }
                Thread.sleep(10);
            }
            throw new AssertionError("AOI did not reach " + sequence + ": " + aoi.metrics(), last);
        }
        @Override public void close() throws Exception {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            rpc.close();
            sessions.close();
            aoi.close();
        }
    }

    private static final class Updates implements StreamObserver<MapViewUpdate> {
        final LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>(16);
        @Override public void onNext(MapViewUpdate update) { assertTrue(queue.offer(update)); }
        @Override public void onError(Throwable failure) { queue.offer(failure); }
        @Override public void onCompleted() { queue.offer("completed"); }
        MapViewUpdate next() throws Exception {
            Object value = queue.poll(8, TimeUnit.SECONDS);
            if (value instanceof MapViewUpdate update) return update;
            throw new AssertionError("Expected map update, got " + value);
        }
        MapViewUpdate through(long sequence) throws Exception {
            for (int i = 0; i < 16; i++) {
                var update = next();
                if (update.getThroughSeq() >= sequence) return update;
            }
            throw new AssertionError("No update reached " + sequence);
        }
    }
}
