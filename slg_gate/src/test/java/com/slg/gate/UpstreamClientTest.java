package com.slg.gate;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import com.slg.rpc.generated.GameplayRoutes;
import com.slg.rpc.v1.*;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class UpstreamClientTest {
    private static final PlayRequest REQUEST = PlayRequest.newBuilder()
            .setMsgId(GameplayRoutes.GET_PLAYER).setRequestId("same-id").build();
    private static final PlayReply OK = PlayReply.newBuilder().setRequestId(REQUEST.getRequestId())
            .setMsgId(REQUEST.getMsgId()).setStatus(200).setBody(ByteString.EMPTY).build();
    private static final AuthReply LOGGED_IN = AuthReply.newBuilder().setStatus(200)
            .setSession(LoginSession.newBuilder().setAccountId("101").setToken("test-session").setExpiresAt(1000)).build();
    private static final String AUTH = "{\"username\":\"alice\",\"password\":\"password-123\"}";

    @Test void budgetsAreIndependentAndBusyCallsAreNeverSent() throws Exception {
        var transport = new ControlledGrpcChannel();
        try (var client = new UpstreamClient(transport, transport, settings(3000))) {
            var uc = client.request("/api/auth/login", "POST", AUTH, null);
            var ucWire = transport.next();
            var game = client.play(REQUEST, "test-session");
            var gameWire = transport.next();
            var busy = client.play(REQUEST, "test-session");
            assertEquals("UPSTREAM_BUSY", UpstreamClient.failure(assertThrows(ExecutionException.class,
                    () -> busy.get(1, TimeUnit.SECONDS))).code());
            assertEquals(2, transport.count.get());
            assertEquals(1, metric(client, "uc", "inFlight"));
            assertEquals(1, metric(client, "game", "inFlight"));
            gameWire.reply(OK); game.get();
            ucWire.reply(LOGGED_IN); uc.get();
            assertEquals(0, metric(client, "game", "inFlight"));
            assertEquals(1, metric(client, "game", "rejected"));
        }
    }

    @Test void typedCommandsCarryProtocolAndBearerAndAllCallsHaveNativeDeadline() throws Exception {
        var transport = new ControlledGrpcChannel();
        try (var client = new UpstreamClient(transport, transport, settings(3000))) {
            var call = client.play(REQUEST, "test-session");
            var wire = transport.next();
            var command = (PlayRequest) wire.request();
            assertEquals(GameplayRoutes.GET_PLAYER, command.getMsgId());
            assertEquals("same-id", command.getRequestId());
            assertTrue(wire.options().getDeadline().timeRemaining(TimeUnit.MILLISECONDS) > 0);
            assertTrue(wire.options().getDeadline().timeRemaining(TimeUnit.MILLISECONDS) <= 3000);
            assertEquals("Bearer test-session", wire.authorization());
            wire.reply(OK); call.get();
            var uc = client.request("/api/auth/login", "POST", AUTH, null);
            var ucWire = transport.next();
            assertNotNull(ucWire.options().getDeadline());
            assertNull(ucWire.authorization());
            ucWire.reply(LOGGED_IN); uc.get();
        }
    }

    @Test void timeoutCancelsOriginalTransportReleasesPermitAndDoesNotReplay() throws Exception {
        var transport = new ControlledGrpcChannel();
        try (var client = new UpstreamClient(transport, transport, settings(150))) {
            var call = client.play(REQUEST, "test-session");
            var wire = transport.next();
            var failure = UpstreamClient.failure(assertThrows(ExecutionException.class, () -> call.get(3, TimeUnit.SECONDS)));
            assertEquals("UPSTREAM_TIMEOUT_UNKNOWN", failure.code());
            assertTrue(failure.getMessage().contains("same requestId"));
            assertTrue(wire.future().isCancelled());
            assertEquals(1, transport.count.get());
            assertEquals(0, metric(client, "game", "inFlight"));
            assertEquals(1, metric(client, "game", "timeouts"));
            assertFalse(call.cancel(true));
            var next = client.play(REQUEST, "test-session");
            transport.next().reply(OK); assertEquals(200, next.get().getStatus());
            assertEquals(0, metric(client, "game", "inFlight"));
        }
    }

    @Test void disconnectCancellationAndCloseReleaseEachPermitExactlyOnce() throws Exception {
        var transport = new ControlledGrpcChannel();
        try (var client = new UpstreamClient(transport, transport, settings(3000))) {
            var call = client.play(REQUEST, "test-session");
            var wire = transport.next();
            assertTrue(call.cancel(true)); assertFalse(call.cancel(true));
            assertTrue(wire.future().isCancelled());
            assertEquals(0, metric(client, "game", "inFlight"));
            assertEquals(1, metric(client, "game", "cancelled"));
            var next = client.play(REQUEST, "test-session");
            var nextWire = transport.next();
            client.close(); client.close();
            assertTrue(next.isCancelled()); assertTrue(nextWire.future().isCancelled());
            assertEquals(0, metric(client, "game", "inFlight"));
            assertEquals(2, metric(client, "game", "cancelled"));
        }
    }

    @Test void nativeFailuresRemainUnknownWhileExplicitBusinessRejectionsRemainDefinite() throws Exception {
        var transport = new ControlledGrpcChannel();
        try (var client = new UpstreamClient(transport, transport, settings(3000))) {
            for (Status status : new Status[] { Status.UNAVAILABLE, Status.RESOURCE_EXHAUSTED, Status.DEADLINE_EXCEEDED }) {
                var call = client.play(REQUEST, "test-session");
                transport.next().fail(status);
                String code = UpstreamClient.failure(assertThrows(ExecutionException.class, call::get)).code();
                assertTrue(code.endsWith("_UNKNOWN"));
                assertEquals(0, metric(client, "game", "inFlight"));
            }
            var call = client.play(REQUEST, "test-session");
            transport.next().reply(OK.toBuilder().setStatus(503).setError(BusinessError.newBuilder()
                    .setCode("SERVER_BUSY").setMessage("Not admitted")).build());
            assertEquals(503, call.get().getStatus());
            assertEquals("SERVER_BUSY", call.get().getError().getCode());
            assertEquals(4, transport.count.get());
        }
    }

    @Test void malformedAuthIsRejectedLocallyWithoutConsumingTheTransport() throws Exception {
        var transport = new ControlledGrpcChannel();
        try (var client = new UpstreamClient(transport, transport, settings(3000))) {
            assertEquals(400, client.request("/api/auth/register", "POST", "{}", null).get().status());
            assertEquals(0, transport.count.get());
            assertEquals(0, metric(client, "uc", "inFlight"));
        }
    }

    @Test void realGrpcRejectsOversizedResponseWithoutRetryAndReleasesBudget() throws Exception {
        var received = new java.util.concurrent.atomic.AtomicInteger();
        var server = com.slg.rpc.RpcTransport.serverBuilder(new java.net.InetSocketAddress("127.0.0.1", 0))
                .addService(new com.slg.rpc.v1.GameRpcGrpc.GameRpcImplBase() {
                    @Override public void play(PlayRequest request, io.grpc.stub.StreamObserver<PlayReply> observer) {
                        received.incrementAndGet();
                        observer.onNext(OK.toBuilder().setBody(ByteString.copyFrom(new byte[1_048_577])).build());
                        observer.onCompleted();
                    }
                }).build().start();
        String target = "127.0.0.1:" + server.getPort();
        try (var client = new UpstreamClient(target, target, settings(3000))) {
            var call = client.play(REQUEST, "test-session");
            assertEquals("UPSTREAM_UNAVAILABLE_UNKNOWN", UpstreamClient.failure(
                    assertThrows(ExecutionException.class, () -> call.get(5, TimeUnit.SECONDS))).code());
            assertEquals(1, received.get());
            assertEquals(0, metric(client, "game", "inFlight"));
        } finally { server.shutdownNow().awaitTermination(3, TimeUnit.SECONDS); }
    }

    @Test void callbackRejectionDrainsFrameworkClosureAndReturnsPermitOnce() throws Exception {
        var transport = new ControlledGrpcChannel();
        var release = new java.util.concurrent.CountDownLatch(1);
        try (var client = new UpstreamClient(transport, transport, settings(5000))) {
            var call = client.play(REQUEST, "test-session");
            var wire = transport.next();
            // 暂停真实回调池并填满队列，触发执行器拒绝；不依赖线程调度快慢碰撞。
            var poolField = UpstreamClient.class.getDeclaredField("callbacks");
            poolField.setAccessible(true);
            var pool = (java.util.concurrent.ThreadPoolExecutor) poolField.get(client);
            var started = new java.util.concurrent.CountDownLatch(pool.getCorePoolSize());
            for (int i = 0; i < pool.getCorePoolSize(); i++) pool.execute(() -> {
                started.countDown();
                try { release.await(); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            });
            assertTrue(started.await(2, TimeUnit.SECONDS));
            int capacity = pool.getQueue().remainingCapacity();
            var drained = new java.util.concurrent.CountDownLatch(capacity);
            for (int i = 0; i < capacity; i++) pool.execute(drained::countDown);

            var frameworkClosed = new java.util.concurrent.atomic.AtomicBoolean();
            wire.options().getExecutor().execute(() -> {
                // 已终态的 observer 必须忽略后到的消息，并继续完成框架关闭。
                wire.listener().onMessage(PlayReply.getDefaultInstance());
                wire.listener().onClose(Status.OK, new io.grpc.Metadata());
                frameworkClosed.set(true);
            });
            assertTrue(frameworkClosed.get(), "Rejected framework work must still drain its terminal callback");
            assertEquals("UPSTREAM_UNAVAILABLE_UNKNOWN", UpstreamClient.failure(
                    assertThrows(ExecutionException.class, call::get)).code());
            assertTrue(wire.future().isCancelled());
            assertEquals(0, metric(client, "game", "inFlight"));
            assertEquals(0, metric(client, "game", "completed"));
            assertFalse(call.cancel(true));

            release.countDown();
            assertTrue(drained.await(2, TimeUnit.SECONDS));
            var next = client.play(REQUEST, "test-session");
            assertEquals(1, metric(client, "game", "inFlight"), "A rejected callback must not return its permit twice");
            transport.next().reply(OK);
            assertEquals(200, next.get().getStatus());
            assertEquals(0, metric(client, "game", "inFlight"));
            assertEquals(1, metric(client, "game", "completed"));
        } finally { release.countDown(); }
    }

    @Test void closeDrainsAlreadyQueuedFrameworkCallbacksInsteadOfDiscardingThem() throws Exception {
        var transport = new ControlledGrpcChannel();
        var release = new java.util.concurrent.CountDownLatch(1);
        try (var client = new UpstreamClient(transport, transport, settings(5000))) {
            var call = client.play(REQUEST, "test-session");
            var wire = transport.next();
            var poolField = UpstreamClient.class.getDeclaredField("callbacks");
            poolField.setAccessible(true);
            var pool = (java.util.concurrent.ThreadPoolExecutor) poolField.get(client);
            var started = new java.util.concurrent.CountDownLatch(pool.getCorePoolSize());
            for (int i = 0; i < pool.getCorePoolSize(); i++) pool.execute(() -> {
                started.countDown();
                try { release.await(); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            });
            assertTrue(started.await(2, TimeUnit.SECONDS));
            var frameworkClosed = new java.util.concurrent.atomic.AtomicBoolean();
            wire.options().getExecutor().execute(() -> {
                wire.listener().onMessage(PlayReply.getDefaultInstance());
                wire.listener().onClose(Status.OK, new io.grpc.Metadata());
                frameworkClosed.set(true);
            });
            assertEquals(1, pool.getQueue().size());
            client.close();
            assertTrue(call.isCancelled());
            assertTrue(wire.future().isCancelled());
            assertEquals(0, metric(client, "game", "inFlight"));
            release.countDown();
            assertTrue(pool.awaitTermination(2, TimeUnit.SECONDS));
            assertTrue(frameworkClosed.get(), "Shutdown must drain a framework task accepted before close");
            assertEquals(1, metric(client, "game", "cancelled"));
            assertEquals(0, metric(client, "game", "completed"));
            var lateCleanup = new java.util.concurrent.atomic.AtomicBoolean();
            wire.options().getExecutor().execute(() -> lateCleanup.set(true));
            assertTrue(lateCleanup.get(), "A framework task arriving after shutdown must also drain");
            assertEquals(0, metric(client, "game", "inFlight"));
        } finally { release.countDown(); }
    }

    static GatewaySettings settings(int timeoutMillis) {
        return new GatewaySettings(100, 6000, 60, 30, 10, timeoutMillis, 1, 1);
    }
    static long metric(UpstreamClient client, String lane, String field) {
        return ((Number) ((Map<?, ?>) client.metrics().get(lane)).get(field)).longValue();
    }
}
