package com.slg.gate;

import com.slg.rpc.v1.*;
import io.grpc.Status;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.*;

/** 会话控制的传输测试使用可控制 RPC 次序，验证 CAS/取消/批量撤销，不依赖 GAME 测试钩子。 */
@Timeout(10)
class SessionUpstreamClientTest {
    private static final String INSTANCE = UUID.randomUUID().toString();
    private static final String TICKET = UUID.randomUUID().toString();
    private static GatewaySettings settings() { return new GatewaySettings(8, 1000, 1000, 30, 10, 3000, 2, 2); }

    @Test void forwardsPrepareTicketAndRevokesMissingBindingFromBoundedBatch() throws Exception {
        var wire = new ControlledGrpcChannel();
        var revoked = new LinkedBlockingQueue<String>();
        var notifications = new LinkedBlockingQueue<Runnable>();
        try (var client = new SessionUpstreamClient(wire, settings())) {
            var open = client.open("credential", "101", reason -> notifications.add(() -> revoked.add(reason)));
            var prepare = wire.next();
            assertInstanceOf(SessionPrepareRequest.class, prepare.request());
            assertEquals("Bearer credential", prepare.authorization());
            prepare.reply(preparation());
            var request = wire.next();
            var body = (SessionOpenRequest) request.request();
            assertEquals(INSTANCE, body.getGameInstance());
            assertEquals(TICKET, body.getTicketId());
            assertEquals(7, body.getExpectedGeneration());
            assertEquals(UUID.fromString(body.getAttemptId()).toString(), body.getAttemptId());
            var binding = binding(30_000);
            assertNotEquals(body.getAttemptId(), binding.getKey().getBindingId());
            request.reply(binding);
            var lease = open.get(1, TimeUnit.SECONDS);
            assertTrue(lease.valid());
            var checking = wire.next();
            var batch = (SessionCheckRequest) checking.request();
            assertEquals(1, batch.getBindingsCount());
            assertEquals(binding.getKey(), batch.getBindings(0));
            checking.reply(SessionCheckReply.newBuilder().setLeaseMillis(30_000).build());
            var notification = notifications.poll(1, TimeUnit.SECONDS);
            assertNotNull(notification);
            assertTrue(revoked.isEmpty(), "TCP EventLoop has not delivered the queued revocation");
            assertFalse(lease.valid());
            assertEquals("SESSION_REPLACED", lease.invalidReason());
            lease.close(); // 本地关闭与撤销竞争不能改写首个原因或重复释放名额。
            assertEquals("SESSION_REPLACED", lease.invalidReason());
            notification.run();
            assertEquals("SESSION_REPLACED", revoked.poll(1, TimeUnit.SECONDS));
            assertEquals(0, client.metrics().get("active"));
            var closing = wire.next();
            assertEquals(binding.getKey(), ((SessionCloseRequest) closing.request()).getBinding());
            closing.reply(SessionCloseReply.getDefaultInstance());
        }
    }

    @Test void cancelledAuthenticationReleasesItsSlotAndNeverOpensAfterLatePreparation() throws Exception {
        var wire = new ControlledGrpcChannel();
        try (var client = new SessionUpstreamClient(wire, settings())) {
            var first = client.open("credential", "101", ignored -> fail("No lease was established"));
            var prepare = wire.next();
            assertTrue(first.cancel(false));
            assertTrue(prepare.future().isCancelled());
            prepare.reply(preparation());
            assertEquals(0, client.metrics().get("opening"));
            assertEquals(0, client.metrics().get("active"));
            var next = client.open("credential", "101", ignored -> {});
            assertInstanceOf(SessionPrepareRequest.class, wire.next().request());
            next.cancel(false);
            assertEquals(0, client.metrics().get("opening"));
        }
    }

    @Test void staleCasFailureIsReturnedWithoutAutomaticReauthentication() throws Exception {
        var wire = new ControlledGrpcChannel();
        try (var client = new SessionUpstreamClient(wire, settings())) {
            var result = client.open("credential", "101", ignored -> {});
            wire.next().reply(preparation());
            wire.next().fail(Status.FAILED_PRECONDITION.withDescription("SESSION_CHANGED"));
            var failure = assertThrows(ExecutionException.class, () -> result.get(1, TimeUnit.SECONDS));
            assertEquals(Status.Code.FAILED_PRECONDITION, Status.fromThrowable(failure.getCause()).getCode());
            assertNull(wire.requests.poll(150, TimeUnit.MILLISECONDS));
            assertEquals(0, client.metrics().get("active"));
            assertEquals(0, client.metrics().get("opening"));
        }
    }

    @Test void invalidRenewalCannotExtendAnUnrelatedSessionAndCapacityIsExplicit() throws Exception {
        var wire = new ControlledGrpcChannel();
        var revoked = new LinkedBlockingQueue<String>();
        try (var client = new SessionUpstreamClient(wire, settings())) {
            var result = client.open("credential", "101", revoked::add);
            wire.next().reply(preparation());
            wire.next().reply(binding(29_500)); // 同一次绑定的回执可能只有剩余租约，不要求恰好30秒。
            var lease = result.get(1, TimeUnit.SECONDS);
            var check = wire.next();
            check.reply(SessionCheckReply.newBuilder().setLeaseMillis(30_000)
                    .addCurrent(SessionTestService.KEY).build());
            assertEquals("SESSION_UNAVAILABLE", revoked.poll(1, TimeUnit.SECONDS));
            assertFalse(lease.valid());
        }
        assertThrows(IllegalArgumentException.class, () -> SessionUpstreamClient.requireCapacity(25_601));
    }

    private static SessionPreparation preparation() {
        return SessionPreparation.newBuilder().setGameInstance(INSTANCE).setObservedGeneration(7).setTicketId(TICKET).build();
    }
    private static SessionBinding binding(int leaseMillis) {
        return SessionBinding.newBuilder().setKey(SessionKey.newBuilder().setAccountId("101").setGameInstance(INSTANCE)
                .setGeneration(8).setBindingId(UUID.randomUUID().toString())).setExpiresAt(2_000_000_000)
                .setLeaseMillis(leaseMillis).build();
    }
}
