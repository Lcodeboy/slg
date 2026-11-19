package com.slg.gate;

import com.slg.rpc.generated.GameplayRoutes;
import com.slg.rpc.RpcTransport;
import com.slg.rpc.v1.MapQueryReply;
import com.slg.rpc.v1.MapQueryRequest;
import com.slg.rpc.v1.MapRpcGrpc;
import com.slg.rpc.v1.MapViewSnapshot;
import com.slg.rpc.v1.MapViewUpdate;
import com.slg.rpc.v1.MapWatchRequest;
import com.slg.rpc.v1.PlayRequest;
import com.slg.rpc.v1.PlayReply;
import com.slg.rpc.v1.GameRpcGrpc;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.*;

/** 地图在途预算和流控使用可控制的真实 gRPC ClientCall 边界验证，不依赖定时 sleep。 */
@Timeout(10)
class MapUpstreamClientTest {
    @Test void mapQueryHasIndependentBudgetAndCancellationReleasesIt() throws Exception {
        var channel = new ControlledGrpcChannel();
        try (var client = new UpstreamClient(channel, channel, UpstreamClientTest.settings(3000))) {
            var gameplay = client.play(PlayRequest.newBuilder().setMsgId(GameplayRoutes.GET_PLAYER)
                    .setRequestId("game").build(), "session", SessionTestService.KEY);
            var gameWire = channel.next();
            var request = MapQueryRequest.getDefaultInstance();
            var map = client.maps().query(request, "session", SessionTestService.KEY);
            var mapWire = channel.next();
            assertEquals(request, mapWire.request());
            assertEquals("Bearer session", mapWire.authorization());
            assertFalse(gameplay.isDone(), "地图查询不能占用已经用满的玩法名额");
            assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> client.maps().query(request, "session", SessionTestService.KEY).get(1, TimeUnit.SECONDS));
            assertTrue(map.cancel(false));
            assertTrue(mapWire.future().isCancelled());
            var next = client.maps().query(request, "session", SessionTestService.KEY);
            channel.next().reply(MapQueryReply.newBuilder()
                    .setSnapshot(MapViewSnapshot.newBuilder().setAppliedSeq(17)).build());
            assertEquals(17, next.get(1, TimeUnit.SECONDS).getSnapshot().getAppliedSeq());
            gameplay.cancel(false);
            assertTrue(gameWire.future().isCancelled());
        }
    }

    @Test void watchRequestsOneMessageAtATimeAndHasNoUnaryDeadline() throws Exception {
        var channel = new ControlledGrpcChannel();
        var updates = new LinkedBlockingQueue<MapViewUpdate>();
        var failures = new LinkedBlockingQueue<Throwable>();
        try (var client = new MapUpstreamClient(channel, UpstreamClientTest.settings(3000))) {
            var watch = client.watch(MapWatchRequest.newBuilder().setSubscriptionId("center").build(),
                    "session", SessionTestService.KEY, updates::add, failures::add);
            var wire = channel.next();
            assertNull(wire.options().getDeadline(), "长期地图订阅不能继承普通 RPC 的三秒截止时间");
            assertEquals(1, wire.requested().get());
            var snapshot = MapViewUpdate.newBuilder().setSubscriptionId("center").setSnapshot(true).build();
            wire.message(snapshot);
            assertEquals(snapshot, updates.poll(1, TimeUnit.SECONDS));
            assertEquals(1, wire.requested().get(), "下游没有确认写完前不能继续向流请求消息");
            watch.requestNext();
            assertEquals(2, wire.requested().get());
            watch.close();
            assertTrue(wire.future().isCancelled());
            assertEquals(0, client.metrics().get("watches"));
            wire.message(snapshot);
            assertTrue(updates.isEmpty(), "已取消流的迟到回调不得再投递");
            assertTrue(failures.isEmpty());
        }
    }

    @Test void wrongSubscriptionAndRemoteFailureReleaseWatchSlot() throws Exception {
        var channel = new ControlledGrpcChannel();
        var failures = new LinkedBlockingQueue<Throwable>();
        try (var client = new MapUpstreamClient(channel, UpstreamClientTest.settings(3000))) {
            client.watch(MapWatchRequest.newBuilder().setSubscriptionId("one").build(), "session", SessionTestService.KEY,
                    update -> fail("Wrong subscription must never reach the observer"), failures::add);
            var wire = channel.next();
            wire.message(MapViewUpdate.newBuilder().setSubscriptionId("other").build());
            assertNotNull(failures.poll(1, TimeUnit.SECONDS));
            assertEquals(0, client.metrics().get("watches"));
            assertTrue(wire.future().isCancelled());
            client.watch(MapWatchRequest.newBuilder().setSubscriptionId("two").build(), "session", SessionTestService.KEY,
                    update -> {}, failures::add);
            channel.next().fail(Status.UNAVAILABLE);
            assertNotNull(failures.poll(1, TimeUnit.SECONDS));
            assertEquals(0, client.metrics().get("watches"));
        }
    }

    @Test void initialSnapshotTimeoutIsBoundedButAnEstablishedWatchHasNoUnaryLifetime() throws Exception {
        var channel = new ControlledGrpcChannel();
        var failures = new LinkedBlockingQueue<Throwable>();
        var updates = new LinkedBlockingQueue<MapViewUpdate>();
        try (var client = new MapUpstreamClient(channel, UpstreamClientTest.settings(150))) {
            client.watch(MapWatchRequest.newBuilder().setSubscriptionId("queued").build(), "session", SessionTestService.KEY, updates::add, failures::add);
            var queued = channel.next();
            var timeout = failures.poll(2, TimeUnit.SECONDS);
            assertNotNull(timeout);
            assertEquals(Status.Code.DEADLINE_EXCEEDED, Status.fromThrowable(timeout).getCode());
            assertTrue(queued.future().isCancelled());
            assertEquals(0, client.metrics().get("watches"));

            var live = client.watch(MapWatchRequest.newBuilder().setSubscriptionId("live").build(), "session", SessionTestService.KEY, updates::add, failures::add);
            var active = channel.next();
            active.message(MapViewUpdate.newBuilder().setSubscriptionId("live").setSnapshot(true).build());
            assertNotNull(updates.poll(1, TimeUnit.SECONDS));
            assertNull(failures.poll(350, TimeUnit.MILLISECONDS), "首包到达后应取消首包定时器，不能在普通 RPC 截止时终止长流");
            assertFalse(active.future().isCancelled());
            assertEquals(1, client.metrics().get("watches"));
            live.close();
        }
    }

    @Test void fullMapHttp2ConnectionCannotBlockGameplayAndQueuedWatchTimesOut() throws Exception {
        var firstCancelled = new CountDownLatch(1);
        var server = RpcTransport.serverBuilder(new InetSocketAddress("127.0.0.1", 0))
                .maxConcurrentCallsPerConnection(1)
                .addService(new MapRpcGrpc.MapRpcImplBase() {
                    @Override public void watch(MapWatchRequest request, StreamObserver<MapViewUpdate> output) {
                        var stream = (ServerCallStreamObserver<MapViewUpdate>) output;
                        if (request.getSubscriptionId().equals("holds-only-map-slot")) {
                            stream.setOnCancelHandler(firstCancelled::countDown);
                            output.onNext(MapViewUpdate.newBuilder().setSubscriptionId(request.getSubscriptionId()).setSnapshot(true).build());
                            // 保持流开放，占住这个 HTTP/2 连接唯一的并发流名额。
                        } else fail("Second map Watch must remain queued behind the first stream");
                    }
                })
                .addService(new GameRpcGrpc.GameRpcImplBase() {
                    @Override public void play(PlayRequest request, StreamObserver<PlayReply> output) {
                        output.onNext(PlayReply.newBuilder().setMsgId(request.getMsgId()).setRequestId(request.getRequestId())
                                .setStatus(200).setBody(com.google.protobuf.ByteString.EMPTY).build());
                        output.onCompleted();
                    }
                }).build().start();
        String target = "127.0.0.1:" + server.getPort();
        var snapshots = new LinkedBlockingQueue<MapViewUpdate>();
        var failures = new LinkedBlockingQueue<Throwable>();
        try (var client = new UpstreamClient(target, target, UpstreamClientTest.settings(2000))) {
            var first = client.maps().watch(MapWatchRequest.newBuilder().setSubscriptionId("holds-only-map-slot").build(),
                    "session", SessionTestService.KEY, snapshots::add, failures::add);
            assertNotNull(snapshots.poll(2, TimeUnit.SECONDS));
            client.maps().watch(MapWatchRequest.newBuilder().setSubscriptionId("queued-map").build(),
                    "session", SessionTestService.KEY, snapshots::add, failures::add);
            var play = client.play(PlayRequest.newBuilder().setMsgId(GameplayRoutes.GET_PLAYER).setRequestId("unblocked").build(), "session", SessionTestService.KEY);
            assertEquals("unblocked", play.get(1, TimeUnit.SECONDS).getRequestId(),
                    "GAME 业务必须使用另一条 HTTP/2 连接，不能等待长期地图流结束");
            var timeout = failures.poll(3, TimeUnit.SECONDS);
            assertNotNull(timeout, "等待地图并发流名额的订阅也必须有首包截止时间");
            assertEquals(Status.Code.DEADLINE_EXCEEDED, Status.fromThrowable(timeout).getCode());
            assertEquals(1, client.maps().metrics().get("watches"));
            first.close();
            assertTrue(firstCancelled.await(1, TimeUnit.SECONDS));
        } finally { server.shutdownNow().awaitTermination(2, TimeUnit.SECONDS); }
    }
}
