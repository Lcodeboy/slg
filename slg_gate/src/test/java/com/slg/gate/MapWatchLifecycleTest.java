package com.slg.gate;

import com.slg.rpc.v1.MapViewUpdate;
import com.slg.rpc.v1.MapWatchRequest;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.*;

/**
 * 本地取消与传输终态刻意分开：业务不再收包后，HTTP/2 流仍可能尚未回收。
 * 用可控制的 ClientCall 验证槽只能归还一次；断流不自动迁移或重新订阅，地图恢复仍由新订阅完成。
 */
@Timeout(10)
class MapWatchLifecycleTest {
    private static GatewaySettings settings(int timeoutMillis) {
        return new GatewaySettings(10, 6000, 60, 30, 10, timeoutMillis, 1, 1, 1, 1);
    }

    private static MapWatchRequest request(String id) {
        return MapWatchRequest.newBuilder().setSubscriptionId(id).build();
    }

    private static int metric(MapUpstreamClient client, String key) {
        return ((Number) client.metrics().get(key)).intValue();
    }

    @Test void cancellationRetainsTransportSlotUntilCloseAndLateCloseCannotReleaseNextLease() throws Exception {
        var wire = new ControlledGrpcChannel();
        var updates = new LinkedBlockingQueue<MapViewUpdate>();
        var failures = new LinkedBlockingQueue<Throwable>();
        try (var client = new MapUpstreamClient(new ControlledGrpcChannel(), List.of(wire), settings(3000))) {
            var first = client.watch(request("first"), "token", SessionTestService.KEY, updates::add, failures::add);
            var firstCall = wire.next();
            first.close();
            first.close();
            assertTrue(firstCall.future().isCancelled());
            assertEquals(0, metric(client, "watches"));
            assertEquals(1, metric(client, "watchSlots"), "本地结束不等于传输终态，不能提前接受替换流");
            assertThrows(RejectedExecutionException.class,
                    () -> client.watch(request("too-early"), "token", SessionTestService.KEY, updates::add, failures::add));
            firstCall.message(MapViewUpdate.newBuilder().setSubscriptionId("first").setSnapshot(true).build());
            assertTrue(updates.isEmpty(), "主动取消后迟到的数据不得交给新视野");

            firstCall.fail(Status.CANCELLED);
            assertEquals(0, metric(client, "watchSlots"));
            var second = client.watch(request("second"), "token", SessionTestService.KEY, updates::add, failures::add);
            var secondCall = wire.next();
            firstCall.fail(Status.CANCELLED);
            first.close();
            assertEquals(1, metric(client, "watchSlots"), "旧 onClose 与重复 close 不能释放新流的槽");
            assertTrue(failures.isEmpty(), "主动取消不应再发失败通知");
            second.close();
            secondCall.fail(Status.CANCELLED);
            assertEquals(0, metric(client, "watchSlots"));
        }
    }

    @Test void initialSnapshotTimeoutCancelsWaitingButKeepsSlotUntilTransportEnds() throws Exception {
        var wire = new ControlledGrpcChannel();
        var failures = new LinkedBlockingQueue<Throwable>();
        try (var client = new MapUpstreamClient(new ControlledGrpcChannel(), List.of(wire), settings(100))) {
            client.watch(request("timeout"), "token", SessionTestService.KEY, value -> fail("No snapshot expected"), failures::add);
            var call = wire.next();
            var failure = failures.poll(2, TimeUnit.SECONDS);
            assertNotNull(failure);
            assertEquals(Status.Code.DEADLINE_EXCEEDED, Status.fromThrowable(failure).getCode());
            assertTrue(call.future().isCancelled());
            assertEquals(0, metric(client, "watches"));
            assertEquals(1, metric(client, "watchSlots"));
            assertThrows(RejectedExecutionException.class,
                    () -> client.watch(request("blocked"), "token", SessionTestService.KEY, value -> {}, failures::add));
            call.fail(Status.CANCELLED);
            assertEquals(0, metric(client, "watchSlots"));
            assertTrue(failures.isEmpty(), "首包超时与迟到终态只能形成一次失败通知");
        }
    }

    @Test void remoteFailureReleasesSlotWithoutOpeningReplacementStream() throws Exception {
        var wire = new ControlledGrpcChannel();
        var failures = new LinkedBlockingQueue<Throwable>();
        try (var client = new MapUpstreamClient(new ControlledGrpcChannel(), List.of(wire), settings(3000))) {
            client.watch(request("failed"), "token", SessionTestService.KEY, value -> {}, failures::add);
            wire.next().fail(Status.UNAVAILABLE);
            assertEquals(Status.Code.UNAVAILABLE, Status.fromThrowable(failures.remove()).getCode());
            assertEquals(0, metric(client, "watchSlots"));
            assertEquals(0, metric(client, "watches"));
            assertEquals(1, wire.count.get());
            assertTrue(wire.requests.isEmpty(), "失败流不得在池内透明重订阅或接上旧水位");
        }
    }

    @Test void newCallFailureDoesNotLeakCapacity() throws Exception {
        verifyConstructionFailure(false);
    }

    @Test void startFailureDoesNotRequireAnOnCloseThatWillNeverArrive() throws Exception {
        verifyConstructionFailure(true);
    }

    private void verifyConstructionFailure(boolean failDuringStart) throws Exception {
        var delegate = new ControlledGrpcChannel();
        var first = new AtomicBoolean(true);
        Channel faulty = new Channel() {
            @Override public String authority() { return delegate.authority(); }
            @Override public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(MethodDescriptor<ReqT, RespT> method, CallOptions options) {
                boolean failThisCall = first.getAndSet(false);
                if (failThisCall && !failDuringStart) throw new IllegalStateException("newCall failed");
                var call = delegate.newCall(method, options);
                return new ForwardingClientCall.SimpleForwardingClientCall<>(call) {
                    @Override public void start(Listener<RespT> listener, Metadata headers) {
                        if (failThisCall) throw new IllegalStateException("start failed before registering listener");
                        super.start(listener, headers);
                    }
                };
            }
        };
        try (var client = new MapUpstreamClient(new ControlledGrpcChannel(), List.of(faulty), settings(3000))) {
            assertThrows(IllegalStateException.class,
                    () -> client.watch(request("broken"), "token", SessionTestService.KEY, value -> {}, failure -> {}));
            assertEquals(0, metric(client, "watchSlots"));
            assertEquals(0, metric(client, "watches"));
            var next = client.watch(request("next"), "token", SessionTestService.KEY, value -> {}, failure -> {});
            var call = delegate.next();
            assertEquals("next", ((MapWatchRequest) call.request()).getSubscriptionId());
            assertEquals(1, metric(client, "watchSlots"));
            next.close();
            call.fail(Status.CANCELLED);
            assertEquals(0, metric(client, "watchSlots"));
        }
    }

    @Test void closeWhileNewCallIsPendingCannotStartStreamAfterReturningLease() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var starts = new AtomicInteger();
        var cancellations = new AtomicInteger();
        var delegate = new ControlledGrpcChannel();
        Channel blocked = new Channel() {
            @Override public String authority() { return delegate.authority(); }
            @Override public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(MethodDescriptor<ReqT, RespT> method, CallOptions options) {
                entered.countDown();
                try {
                    if (!release.await(3, TimeUnit.SECONDS)) throw new AssertionError("newCall was not released");
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(interrupted);
                }
                return new ForwardingClientCall.SimpleForwardingClientCall<>(delegate.newCall(method, options)) {
                    @Override public void start(Listener<RespT> listener, Metadata headers) {
                        starts.incrementAndGet();
                        super.start(listener, headers);
                    }
                    @Override public void cancel(String message, Throwable cause) {
                        cancellations.incrementAndGet();
                        super.cancel(message, cause);
                    }
                };
            }
        };
        var client = new MapUpstreamClient(new ControlledGrpcChannel(), List.of(blocked), settings(3000));
        try (var executor = Executors.newSingleThreadExecutor()) {
            var creating = executor.submit(() -> client.watch(request("closing"), "token", SessionTestService.KEY,
                    value -> fail("Closed client cannot deliver updates"), failure -> fail("Local close is not a remote failure")));
            try {
                assertTrue(entered.await(2, TimeUnit.SECONDS));
                assertEquals(1, metric(client, "watchSlots"));
                client.close();
                assertEquals(0, metric(client, "watchSlots"));
                release.countDown();
                creating.get(2, TimeUnit.SECONDS).close();
                assertEquals(0, starts.get(), "归还槽后晚到的 ClientCall 必须取消，不能再启动");
                assertTrue(cancellations.get() > 0);
                assertEquals(0, delegate.count.get());
                assertEquals(0, metric(client, "watchSlots"));
                assertEquals(0, metric(client, "watches"));
                assertThrows(RejectedExecutionException.class,
                        () -> client.watch(request("after-close"), "token", SessionTestService.KEY, value -> {}, failure -> {}));
            } finally {
                release.countDown();
                client.close();
            }
        }
    }
}
