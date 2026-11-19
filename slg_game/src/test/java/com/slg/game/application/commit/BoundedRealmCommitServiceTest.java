package com.slg.game.application.commit;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static com.slg.game.application.commit.CommitServiceTestSupport.*;
import static org.junit.jupiter.api.Assertions.*;

/** 闩锁固定启动/取消/通知竞争顺序，验证有界准入与确定终态，不用吞吐计数代替正确性。 */
@Timeout(30)
class BoundedRealmCommitServiceTest {
    @Test void invalidPayloadMeasurementRejectsBeforeAnySqlOrCapacityReservation() throws Exception {
        var tx = new Transactions();
        var service = new BoundedRealmCommitService(WRITER, tx, new Ownership(), request -> {
            if (request.decision().request().requestId().equals("invalid")) throw new IllegalArgumentException("Unsupported payload");
            return 0;
        }, limits(4, 1000, 1, 100, 1, 1));
        try {
            assertEquals(CommitResult.Reason.INVALID_DECISION, reason(service.submit(request("invalid"))));
            assertEquals(CommitResult.Reason.INVALID_DECISION, reason(service.submit(request("zero-size"))));
            assertTrue(tx.calls.isEmpty()); assertEquals(0, service.snapshot().acceptedTasks());
            assertEquals(0, service.snapshot().retainedBytes());
        } finally { drain(service); }
    }

    @Test void zeroWaitingBudgetCanUseAnImmediatelyAvailableDatabaseSlot() throws Exception {
        var tx = new Transactions();
        var service = new BoundedRealmCommitService(WRITER, tx, new Ownership(), request -> 100, limits(4, 1000, 1, 100, 1, 1));
        try {
            assertInstanceOf(CommitResult.Committed.class,
                    result(service.submit(request("immediate", CommitRequest.Origin.CLIENT, Duration.ZERO))));
            assertEquals(List.of("immediate"), tx.calls);
        } finally { drain(service); }
    }

    @Test void taskLimitIncludesExecutingAndPreservesSystemAdmission() throws Exception {
        var tx = new Transactions(); var ownership = new Ownership(); var release = new CountDownLatch(1);
        tx.commitAction = (writer, decision) -> { gate(release); return tx.committed(writer, decision); };
        var service = new BoundedRealmCommitService(WRITER, tx, ownership, request -> 100, limits(3, 1000, 1, 100, 1, 1));
        try {
            service.submit(request("first")); await(() -> tx.calls.size() == 1);
            service.submit(request("second"));
            assertEquals(CommitResult.Reason.QUEUE_FULL, reason(service.submit(request("client-overflow"))));
            service.submit(request("timer", CommitRequest.Origin.DUE_TASK, Duration.ofSeconds(5)));
            assertEquals(3, service.snapshot().acceptedTasks()); assertEquals(300, service.snapshot().retainedBytes());
            assertEquals(CommitResult.Reason.QUEUE_FULL,
                    reason(service.submit(request("battle-overflow", CommitRequest.Origin.BATTLE_RESULT, Duration.ofSeconds(5)))));
        } finally { release.countDown(); drain(service); }
    }

    @Test void encodedByteLimitIsIndependentOfTaskCountAndAlsoReservesSystemBytes() throws Exception {
        var tx = new Transactions(); var release = new CountDownLatch(1);
        tx.commitAction = (writer, decision) -> { gate(release); return tx.committed(writer, decision); };
        var service = new BoundedRealmCommitService(WRITER, tx, new Ownership(), request -> 100, limits(10, 500, 1, 200, 1, 1));
        try {
            service.submit(request("a")); await(() -> tx.calls.size() == 1);
            service.submit(request("b")); service.submit(request("c"));
            assertEquals(CommitResult.Reason.QUEUE_FULL, reason(service.submit(request("client-overflow"))));
            service.submit(request("timer", CommitRequest.Origin.DUE_TASK, Duration.ofSeconds(5)));
            service.submit(request("battle", CommitRequest.Origin.BATTLE_RESULT, Duration.ofSeconds(5)));
            assertEquals(5, service.snapshot().acceptedTasks()); assertEquals(500, service.snapshot().retainedBytes());
            assertEquals(CommitResult.Reason.QUEUE_FULL,
                    reason(service.submit(request("admin-overflow", CommitRequest.Origin.ADMIN, Duration.ofSeconds(5)))));
        } finally { release.countDown(); drain(service); }
    }

    @Test void queuedCancellationAndZeroWaitNeverStartSqlLater() throws Exception {
        var tx = new Transactions(); var release = new CountDownLatch(1);
        tx.commitAction = (writer, decision) -> { gate(release); return tx.committed(writer, decision); };
        var service = new BoundedRealmCommitService(WRITER, tx, new Ownership(), request -> 100, limits(8, 1000, 1, 100, 1, 1));
        try {
            var running = service.submit(request("running")); await(() -> tx.calls.size() == 1);
            assertFalse(running.cancelBeforeStart());
            var cancelled = service.submit(request("cancelled"));
            assertTrue(cancelled.cancelBeforeStart()); assertFalse(cancelled.cancelBeforeStart());
            assertEquals(CommitResult.Reason.CANCELLED, reason(cancelled));
            assertEquals(CommitResult.Reason.QUEUE_TIMEOUT,
                    reason(service.submit(request("no-wait", CommitRequest.Origin.CLIENT, Duration.ZERO))));
            release.countDown(); result(running); drain(service);
            assertEquals(List.of("running"), tx.calls);
        } finally { release.countDown(); drain(service); }
    }

    @Test void waitingBudgetExpiresWithoutReinterpretingAnExecutingAttempt() throws Exception {
        var tx = new Transactions(); var release = new CountDownLatch(1);
        tx.commitAction = (writer, decision) -> { gate(release); return tx.committed(writer, decision); };
        var service = new BoundedRealmCommitService(WRITER, tx, new Ownership(), request -> 100, limits(8, 1000, 1, 100, 1, 1));
        try {
            var running = service.submit(request("running")); await(() -> tx.calls.size() == 1);
            var timeout = service.submit(request("expired", CommitRequest.Origin.CLIENT, Duration.ofMillis(40)));
            assertEquals(CommitResult.Reason.QUEUE_TIMEOUT, reason(timeout));
            assertFalse(running.settlement().toCompletableFuture().isDone());
            assertEquals(List.of("running"), tx.calls);
        } finally { release.countDown(); drain(service); }
    }

    @Test void sameRequestIsSerializedWhileAnIndependentRequestCanUseAnotherWorker() throws Exception {
        var tx = new Transactions(); var release = new CountDownLatch(1); var sameCalls = new AtomicInteger();
        tx.commitAction = (writer, decision) -> {
            if (decision.request().requestId().equals("same") && sameCalls.incrementAndGet() == 1) gate(release);
            return tx.committed(writer, decision);
        };
        var service = new BoundedRealmCommitService(WRITER, tx, new Ownership(), request -> 100, limits(8, 1000, 1, 100, 2, 2));
        try {
            var first = service.submit(request("same")); await(() -> sameCalls.get() == 1);
            var second = service.submit(request("same"));
            assertInstanceOf(CommitResult.Committed.class, result(service.submit(request("independent"))));
            assertEquals(1, sameCalls.get()); assertEquals(CommitPhase.QUEUED, second.phase());
            release.countDown(); result(first); result(second); assertEquals(2, sameCalls.get());
        } finally { release.countDown(); drain(service); }
    }

    @Test void originRotationPreventsClientOrSystemBacklogsFromStarvingOtherOrigins() throws Exception {
        var tx = new Transactions(); var release = new CountDownLatch(1);
        tx.commitAction = (writer, decision) -> { if (decision.request().requestId().equals("head")) gate(release); return tx.committed(writer, decision); };
        var service = new BoundedRealmCommitService(WRITER, tx, new Ownership(), request -> 100, limits(24, 5000, 4, 400, 1, 2));
        var handles = new ArrayList<CommitHandle>();
        try {
            handles.add(service.submit(request("head"))); await(() -> tx.calls.size() == 1);
            for (var origin : CommitRequest.Origin.values()) for (int i = 0; i < 3; i++)
                handles.add(service.submit(request(origin.name() + i, origin, Duration.ofSeconds(5))));
            release.countDown(); for (var handle : handles) result(handle);
            for (var origin : CommitRequest.Origin.values())
                assertTrue(tx.calls.indexOf(origin.name() + 0) <= 4, "Every origin must run within one scheduling rotation");
        } finally { release.countDown(); drain(service); }
    }

    @Test void cancellingAnExternalFutureDoesNotCancelTheUnderlyingCommit() throws Exception {
        var tx = new Transactions(); var release = new CountDownLatch(1);
        tx.commitAction = (writer, decision) -> { gate(release); return tx.committed(writer, decision); };
        var service = new BoundedRealmCommitService(WRITER, tx, new Ownership(), request -> 100, limits(4, 1000, 1, 100, 1, 1));
        try {
            var handle = service.submit(request("future")); await(() -> tx.calls.size() == 1);
            assertTrue(handle.settlement().toCompletableFuture().cancel(true));
            assertEquals(CommitPhase.EXECUTING, handle.phase());
            release.countDown(); assertInstanceOf(CommitResult.Committed.class, result(handle));
            assertEquals(1, tx.calls.size());
        } finally { release.countDown(); drain(service); }
    }

    @Test void blockedUserCallbackRetainsAdmissionButNeverOccupiesTheJdbcWorker() throws Exception {
        var tx = new Transactions(); var releaseSql = new CountDownLatch(1); var releaseCallback = new CountDownLatch(1);
        var callbackEntered = new CountDownLatch(1);
        tx.commitAction = (writer, decision) -> { if (decision.request().requestId().equals("first")) gate(releaseSql); return tx.committed(writer, decision); };
        var service = new BoundedRealmCommitService(WRITER, tx, new Ownership(), request -> 100, limits(2, 200, 0, 0, 1, 1));
        try {
            var first = service.submit(request("first")); await(() -> tx.calls.size() == 1);
            first.settlement().thenRun(() -> {
                callbackEntered.countDown();
                try { assertTrue(releaseCallback.await(10, TimeUnit.SECONDS)); }
                catch (InterruptedException error) { Thread.currentThread().interrupt(); throw new AssertionError(error); }
            });
            releaseSql.countDown(); assertTrue(callbackEntered.await(3, TimeUnit.SECONDS));
            var second = service.submit(request("second")); await(() -> tx.calls.size() == 2);
            assertEquals(CommitResult.Reason.QUEUE_FULL, reason(service.submit(request("overflow"))));
            assertEquals(2, service.snapshot().acceptedTasks());
            var drained = service.drain().toCompletableFuture(); assertFalse(drained.isDone());
            releaseCallback.countDown(); result(second); drained.get(5, TimeUnit.SECONDS);
        } finally { releaseSql.countDown(); releaseCallback.countDown(); drain(service); }
    }

    @Test void publicationBarriersReleaseIndependentlyAndStoppingRejectsNewWork() throws Exception {
        var service = new BoundedRealmCommitService(WRITER, new Transactions(), new Ownership(), request -> 100, limits(4, 1000, 1, 100, 1, 1));
        var first = service.isolatePublication(); var second = service.isolatePublication();
        try {
            assertTrue(service.currentWriter().isEmpty()); assertEquals(2, service.snapshot().publicationHolds());
            assertEquals(CommitResult.Reason.ISOLATED, reason(service.submit(request("paused"))));
            first.releaseAfterReload(); first.releaseAfterReload();
            assertEquals(1, service.snapshot().publicationHolds()); assertTrue(service.currentWriter().isEmpty());
            second.releaseAfterReload(); await(() -> service.currentWriter().isPresent());
            drain(service); assertEquals(CommitResult.Reason.STOPPING, reason(service.submit(request("stopped"))));
        } finally { first.releaseAfterReload(); second.releaseAfterReload(); drain(service); }
    }
}
