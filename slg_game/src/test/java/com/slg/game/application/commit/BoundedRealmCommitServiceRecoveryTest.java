package com.slg.game.application.commit;

import com.slg.game.application.port.RealmTransaction;
import com.slg.game.application.port.RealmOwnership;
import com.slg.game.domain.persistence.CommitReceipt;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static com.slg.game.application.commit.CommitServiceTestSupport.*;
import static org.junit.jupiter.api.Assertions.*;

/** UNKNOWN 只经数据库写者屏障和回执核对收敛；故障期间不能完成 Future 或释放同请求资格。 */
@Timeout(30)
class BoundedRealmCommitServiceRecoveryTest {
    @Test void oneBarrierCoversAllOldEpochExecutionsIncludingLateUnknownResults() throws Exception {
        var tx = new Transactions(); var ownership = new Ownership(); ownership.pauseFence = true;
        var releaseFirst = new CountDownLatch(1); var releaseSecond = new CountDownLatch(1);
        tx.commitAction = (writer, decision) -> {
            gate(decision.request().requestId().equals("first") ? releaseFirst : releaseSecond);
            return new RealmTransaction.CommitAttempt.Uncertain();
        };
        tx.resolveAction = (previous, next, key, digest) -> new RealmTransaction.CommitAttempt.Settled(
                new CommitResult.Committed(new CommitReceipt(key, tx.sequence.incrementAndGet(), digest, "{\"ok\":true}"), false));
        var service = new BoundedRealmCommitService(WRITER, tx, ownership, request -> 100, limits(8, 2000, 1, 100, 2, 2));
        try {
            var first = service.submit(request("first")); var second = service.submit(request("second"));
            await(() -> tx.calls.size() == 2);
            var queued = service.submit(request("never-started"));
            releaseFirst.countDown(); await(() -> first.phase() == CommitPhase.RECONCILING);
            assertEquals(0, ownership.fences.get(), "Fence waits until every old-epoch execution has reported");
            assertTrue(service.currentWriter().isEmpty()); assertFalse(first.cancelBeforeStart());
            assertInstanceOf(CommitResult.NotCommitted.class, result(queued));
            assertFalse(first.settlement().toCompletableFuture().isDone());
            releaseSecond.countDown(); await(() -> ownership.fences.get() == 1);
            assertEquals(2, service.snapshot().reconciling());
            ownership.fenceGate.countDown();
            assertFalse(assertInstanceOf(CommitResult.Committed.class, result(first)).applied());
            assertFalse(assertInstanceOf(CommitResult.Committed.class, result(second)).applied());
            await(() -> service.currentWriter().isPresent());
            assertEquals(2, service.currentWriter().orElseThrow().epoch());
            assertEquals(1, ownership.fences.get()); assertEquals(2, tx.resolutions.size());
            tx.resolutions.forEach(resolution -> {
                assertEquals(WRITER, resolution.previous()); assertEquals(2, resolution.replacement().epoch());
            });
            assertEquals(2, tx.calls.size(), "Recovery must never re-execute a business decision");
        } finally {
            releaseFirst.countDown(); releaseSecond.countDown(); ownership.fenceGate.countDown(); drain(service);
        }
    }

    @Test void lostFenceAcknowledgementStopsRenewingAndWaitsForExpiryBeforeFreshAcquire() throws Exception {
        var tx = new Transactions(); var ownership = new Ownership(); ownership.loseFenceAcknowledgement = true;
        tx.commitAction = (writer, decision) -> new RealmTransaction.CommitAttempt.Uncertain();
        var service = new BoundedRealmCommitService(WRITER, tx, ownership, request -> 100, limits(4, 1000, 1, 100, 1, 1));
        try {
            var handle = service.submit(request("unknown"));
            await(() -> ownership.observations.get() >= 2);
            assertEquals(2, ownership.current.get().epoch());
            assertEquals(0, ownership.acquires.get()); assertEquals(0, ownership.reacquires.get()); assertEquals(0, ownership.renewals.get());
            assertTrue(service.currentWriter().isEmpty()); assertFalse(handle.settlement().toCompletableFuture().isDone());
            var drained = service.drain().toCompletableFuture(); assertFalse(drained.isDone());
            ownership.leaseValid.set(false);
            assertEquals(CommitResult.Reason.ROLLED_BACK, reason(handle));
            drained.get(5, TimeUnit.SECONDS);
            assertEquals(1, ownership.fences.get()); assertEquals(0, ownership.acquires.get()); assertEquals(1, ownership.reacquires.get());
            assertEquals(3, tx.resolutions.getFirst().replacement().epoch());
            assertEquals(1, tx.calls.size());
        } finally { ownership.leaseValid.set(false); drain(service); }
    }

    @Test void failedReconciliationRetriesObservationWithoutRepeatingCommitOrFence() throws Exception {
        var tx = new Transactions(); var ownership = new Ownership(); var attempts = new AtomicInteger();
        var allowResult = new CountDownLatch(1);
        tx.commitAction = (writer, decision) -> new RealmTransaction.CommitAttempt.Uncertain();
        tx.resolveAction = (previous, next, key, digest) -> {
            if (attempts.incrementAndGet() < 3) throw unavailable();
            gate(allowResult);
            return new RealmTransaction.CommitAttempt.Settled(new CommitResult.NotCommitted(CommitResult.Reason.ROLLED_BACK));
        };
        var service = new BoundedRealmCommitService(WRITER, tx, ownership, request -> 100, limits(4, 1000, 1, 100, 1, 1));
        try {
            var handle = service.submit(request("retry-resolution")); await(() -> attempts.get() >= 3);
            assertEquals(CommitPhase.RECONCILING, handle.phase()); assertEquals(1, tx.calls.size());
            assertEquals(1, ownership.fences.get()); assertFalse(handle.settlement().toCompletableFuture().isDone());
            var drained = service.drain().toCompletableFuture(); assertFalse(drained.isDone());
            allowResult.countDown(); assertEquals(CommitResult.Reason.ROLLED_BACK, reason(handle));
            drained.get(5, TimeUnit.SECONDS);
            assertEquals(1, tx.calls.size()); assertEquals(1, ownership.fences.get());
        } finally { allowResult.countDown(); drain(service); }
    }

    @Test void foreignHigherEpochHaltsAutomaticRecoveryWithoutPretendingUnknownWasDrained() throws Exception {
        var tx = new Transactions(); var ownership = new Ownership(); ownership.foreignAfterFence = UUID.randomUUID();
        tx.commitAction = (writer, decision) -> new RealmTransaction.CommitAttempt.Uncertain();
        var service = new BoundedRealmCommitService(WRITER, tx, ownership, request -> 100, limits(4, 1000, 1, 100, 1, 1));
        var handle = service.submit(request("foreign-owner"));
        await(() -> service.snapshot().mode() == CommitQueueSnapshot.Mode.OWNER_LOST);
        assertEquals(CommitPhase.RECONCILING, handle.phase()); assertTrue(service.currentWriter().isEmpty());
        assertFalse(handle.settlement().toCompletableFuture().isDone());
        int observations = ownership.observations.get(), renewals = ownership.renewals.get(), acquires = ownership.acquires.get();
        Thread.sleep(100);
        assertEquals(observations, ownership.observations.get()); assertEquals(renewals, ownership.renewals.get());
        assertEquals(acquires, ownership.acquires.get()); assertEquals(0, acquires);
        assertEquals(0, ownership.reacquires.get());
        assertInstanceOf(CommitResult.NotCommitted.class, result(service.submit(request("must-not-write"))));
        assertFalse(service.drain().toCompletableFuture().isDone());
        assertEquals(1, tx.calls.size()); assertTrue(tx.resolutions.isEmpty());
        // OWNER_LOST 已停内部后台任务；未知句柄仍未确定，不能为了测试清理把它伪造成 ROLLED_BACK。
    }

    @Test void inactiveOwnerWithErasedIdentityCannotBeAutomaticallyAcquiredDuringRecovery() throws Exception {
        for (long observedEpoch : new long[]{WRITER.epoch(), WRITER.epoch() + 2}) {
            var tx = new Transactions(); var ownership = new Ownership();
            ownership.loseFenceAcknowledgement = true; ownership.leaseValid.set(false);
            // 同任期被释放，或其他实例曾取得更高任期后又释放，都已无法证实仍属本实例。
            ownership.observationOverride = new RealmOwnership.OwnershipObservation(
                    WRITER.realm(), observedEpoch, null, false, false);
            tx.commitAction = (writer, decision) -> new RealmTransaction.CommitAttempt.Uncertain();
            var service = new BoundedRealmCommitService(WRITER, tx, ownership, request -> 100, limits(4, 1000, 1, 100, 1, 1));
            var handle = service.submit(request("released-owner-" + observedEpoch));
            await(() -> service.snapshot().mode() == CommitQueueSnapshot.Mode.OWNER_LOST);
            assertEquals(0, ownership.acquires.get()); assertEquals(0, ownership.reacquires.get()); assertEquals(0, ownership.renewals.get());
            assertEquals(1, ownership.fences.get()); assertTrue(tx.resolutions.isEmpty());
            assertTrue(service.currentWriter().isEmpty()); assertEquals(CommitPhase.RECONCILING, handle.phase());
            assertFalse(handle.settlement().toCompletableFuture().isDone());
            assertEquals(CommitResult.Reason.OWNER_LOST, reason(service.submit(request("must-not-reacquire"))));
            int observations = ownership.observations.get();
            Thread.sleep(100);
            assertEquals(observations, ownership.observations.get());
            assertEquals(0, ownership.acquires.get()); assertEquals(0, ownership.reacquires.get());
            assertFalse(service.drain().toCompletableFuture().isDone());
        }
    }
}
