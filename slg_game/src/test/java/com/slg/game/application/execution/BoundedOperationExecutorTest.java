package com.slg.game.application.execution;

import com.slg.core.battle.TroopType;
import com.slg.game.application.commit.*;
import com.slg.game.application.port.CommitPayloadSizer;
import com.slg.game.domain.persistence.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class BoundedOperationExecutorTest {
    private static final RealmIdentity REALM = new RealmIdentity(1, UUID.randomUUID());
    private static final WriterToken WRITER = new WriterToken(REALM, UUID.randomUUID(), 1);

    @Test void differentTroopsOfOnePlayerCannotPrepareBeforeThePreviousPublication() throws Exception {
        var coordinator = new Coordinator();
        var secondPrepared = new CountDownLatch(1);
        var publishEntered = new CountDownLatch(1);
        var allowPublish = new CountDownLatch(1);
        var executor = executor(coordinator, new Handler() {
            @Override public CommitDecision prepare(OperationCommand command) {
                if (command.scope().request().requestId().equals("second")) secondPrepared.countDown();
                return decision(command);
            }
            @Override public void publish(OperationCommand command, CommitDecision decision, CommitResult.Committed result) throws Exception {
                if (command.scope().request().requestId().equals("first")) { publishEntered.countDown(); await(allowPublish); }
            }
        });
        try {
            var first = executor.execute(troop("first", TroopType.INFANTRY));
            var firstCommit = coordinator.take();
            var second = executor.execute(troop("second", TroopType.MARKSMAN));
            assertEquals(OperationPhase.WAITING, second.phase());
            assertEquals(1, secondPrepared.getCount());
            firstCommit.committed(true);
            await(publishEntered);
            assertEquals(OperationPhase.WAITING, second.phase());
            assertEquals(1, secondPrepared.getCount());
            allowPublish.countDown();
            assertInstanceOf(CommitResult.Committed.class, result(first));
            await(secondPrepared);
            coordinator.take().committed(true);
            assertInstanceOf(CommitResult.Committed.class, result(second));
        } finally { allowPublish.countDown(); drain(executor); }
    }

    @Test void independentObjectsPrepareConcurrentlyWhileBothScopesAreHeld() throws Exception {
        var coordinator = new Coordinator();
        var both = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        var executor = executor(coordinator, new Handler() {
            @Override public CommitDecision prepare(OperationCommand command) throws Exception {
                both.countDown(); await(release); return decision(command);
            }
        });
        try {
            var first = executor.execute(command("a", "1_7", CommitRequest.Origin.CLIENT));
            var second = executor.execute(command("b", "1_8", CommitRequest.Origin.CLIENT));
            await(both);
            assertEquals(2, executor.stats().preparing());
            release.countDown();
            coordinator.take().committed(true);
            coordinator.take().committed(true);
            result(first); result(second);
        } finally { release.countDown(); drain(executor); }
    }

    @Test void unrelatedSystemTaskBypassesBlockedPlayerButCannotOvertakeSamePlayer() throws Exception {
        var coordinator = new Coordinator();
        var executor = executor(coordinator, new Handler());
        try {
            var first = executor.execute(command("first", "1_7", CommitRequest.Origin.CLIENT));
            var firstCommit = coordinator.take();
            var second = executor.execute(command("second", "1_7", CommitRequest.Origin.CLIENT));
            var samePlayerSystem = executor.execute(command("third", "1_7", CommitRequest.Origin.DUE_TASK));
            var independent = executor.execute(command("independent", "1_8", CommitRequest.Origin.DUE_TASK));
            var independentCommit = coordinator.take();
            assertEquals("independent", independentCommit.request.request().requestId());
            independentCommit.committed(true); result(independent);
            firstCommit.committed(true); result(first);
            var secondCommit = coordinator.take();
            assertEquals("second", secondCommit.request.request().requestId());
            secondCommit.committed(true); result(second);
            assertEquals(OperationPhase.COMMITTING, eventuallySubmitted(coordinator, samePlayerSystem, "third"));
            result(samePlayerSystem);
        } finally { drain(executor); }
    }

    @Test void unknownAndCancelledClientFutureKeepQualificationsAndDrainPending() throws Exception {
        var coordinator = new Coordinator();
        var executor = executor(coordinator, new Handler());
        var first = executor.execute(command("first", "1_7", CommitRequest.Origin.CLIENT));
        var unresolved = coordinator.take();
        unresolved.phase = CommitPhase.RECONCILING;
        first.completion().toCompletableFuture().cancel(true);
        var second = executor.execute(command("second", "1_7", CommitRequest.Origin.BATTLE_RESULT));
        assertEquals(OperationPhase.WAITING, second.phase());
        assertFalse(first.cancelBeforeStart());
        var draining = executor.drain().toCompletableFuture();
        assertEquals(CommitResult.Reason.STOPPING, rejected(result(second)));
        assertFalse(draining.isDone());
        assertEquals(OperationPhase.COMMITTING, first.phase());
        unresolved.rejected(CommitResult.Reason.ROLLED_BACK);
        assertEquals(CommitResult.Reason.ROLLED_BACK, rejected(result(first)));
        draining.get(3, TimeUnit.SECONDS);
    }

    @Test void publishFailureIsolatesUntilReloadCompletesWithoutChangingCommittedResult() throws Exception {
        var coordinator = new Coordinator();
        var repairEntered = new CountDownLatch(1);
        var allowRepair = new CountDownLatch(1);
        var executor = executor(coordinator, new Handler() {
            @Override public void publish(OperationCommand command, CommitDecision decision, CommitResult.Committed result) { throw new IllegalStateException("publish"); }
            @Override public void reload(OperationCommand command, CommitDecision decision, CommitResult.Committed result) throws Exception {
                repairEntered.countDown(); await(allowRepair);
            }
        });
        try {
            var handle = executor.execute(command("first", "1_7", CommitRequest.Origin.CLIENT));
            coordinator.take().committed(true);
            await(repairEntered);
            assertEquals(1, coordinator.barriers.get());
            assertFalse(handle.completion().toCompletableFuture().isDone());
            assertEquals(CommitResult.Reason.ISOLATED, rejected(result(executor.execute(command("other", "1_8", CommitRequest.Origin.CLIENT)))));
            allowRepair.countDown();
            assertTrue(assertInstanceOf(CommitResult.Committed.class, result(handle)).applied());
            assertEquals(0, coordinator.barriers.get());
        } finally { allowRepair.countDown(); drain(executor); }
    }

    @Test void failedReloadRetainsScopeAndRequiresExplicitRepairBeforeDrainCanComplete() throws Exception {
        var coordinator = new Coordinator();
        var attempts = new AtomicInteger();
        var executor = executor(coordinator, new Handler() {
            @Override public void publish(OperationCommand command, CommitDecision decision, CommitResult.Committed result) { throw new IllegalStateException("publish"); }
            @Override public void reload(OperationCommand command, CommitDecision decision, CommitResult.Committed result) {
                if (attempts.incrementAndGet() == 1) throw new IllegalStateException("reload");
            }
        });
        var handle = executor.execute(command("first", "1_7", CommitRequest.Origin.CLIENT));
        coordinator.take().committed(true);
        await(() -> handle.phase() == OperationPhase.ISOLATED);
        assertEquals(1, executor.stats().isolated());
        assertEquals(1, coordinator.barriers.get());
        var draining = executor.drain().toCompletableFuture();
        assertFalse(draining.isDone());
        assertTrue(executor.retryRepair(handle.requestKey()));
        assertInstanceOf(CommitResult.Committed.class, result(handle));
        draining.get(3, TimeUnit.SECONDS);
        assertEquals(2, attempts.get());
        assertEquals(0, coordinator.barriers.get());
        assertFalse(executor.retryRepair(handle.requestKey()));
    }

    @Test void replayReloadsAuthorityInsteadOfPublishingThePreparedDraft() throws Exception {
        var coordinator = new Coordinator();
        var publishes = new AtomicInteger();
        var reloads = new AtomicInteger();
        var executor = executor(coordinator, new Handler() {
            @Override public void publish(OperationCommand command, CommitDecision decision, CommitResult.Committed result) { publishes.incrementAndGet(); }
            @Override public void reload(OperationCommand command, CommitDecision decision, CommitResult.Committed result) { reloads.incrementAndGet(); }
        });
        try {
            var handle = executor.execute(command("replay", "1_7", CommitRequest.Origin.CLIENT));
            coordinator.take().committed(false);
            assertFalse(assertInstanceOf(CommitResult.Committed.class, result(handle)).applied());
            assertEquals(0, publishes.get()); assertEquals(1, reloads.get());
        } finally { drain(executor); }
    }

    @Test void waitingCancellationNeverRunsPreparationOrSubmitsTwice() throws Exception {
        var coordinator = new Coordinator();
        var prepared = new AtomicInteger();
        var executor = executor(coordinator, new Handler() {
            @Override public CommitDecision prepare(OperationCommand command) { prepared.incrementAndGet(); return decision(command); }
        });
        try {
            var first = executor.execute(command("first", "1_7", CommitRequest.Origin.CLIENT));
            var firstCommit = coordinator.take();
            var cancelled = executor.execute(command("second", "1_7", CommitRequest.Origin.CLIENT));
            assertTrue(cancelled.cancelBeforeStart());
            assertFalse(cancelled.cancelBeforeStart());
            assertEquals(CommitResult.Reason.CANCELLED, rejected(result(cancelled)));
            assertEquals(1, prepared.get());
            firstCommit.committed(true); result(first);
            assertEquals(1, coordinator.submissions.get());
        } finally { drain(executor); }
    }

    @Test void theSameRequestSerializesEvenWhenItsExplicitObjectSetsDiffer() throws Exception {
        var coordinator = new Coordinator();
        var executor = executor(coordinator, new Handler());
        try {
            var first = executor.execute(command("same", "1_7", CommitRequest.Origin.CLIENT));
            var firstCommit = coordinator.take();
            var second = executor.execute(command("same", "1_8", CommitRequest.Origin.ADMIN));
            assertEquals(OperationPhase.WAITING, second.phase());
            assertEquals(1, coordinator.submissions.get());
            firstCommit.committed(true); result(first);
            coordinator.take().committed(false);
            assertFalse(assertInstanceOf(CommitResult.Committed.class, result(second)).applied());
        } finally { drain(executor); }
    }

    @Test void scopeValidationAndPreparationFailureReleaseOnlyBeforeSubmission() throws Exception {
        var coordinator = new Coordinator();
        var executor = executor(coordinator, new Handler() {
            @Override public CommitDecision prepare(OperationCommand command) {
                if (command.scope().request().requestId().equals("bad"))
                    return new CommitDecision(command.scope().request(), "digest", "FACT", "{}", "{}", List.of(),
                            Map.of(new ObjectKey.GatherSite(1, 2), 0L));
                return decision(command);
            }
        });
        try {
            assertEquals(CommitResult.Reason.INVALID_DECISION,
                    rejected(result(executor.execute(command("bad", "1_7", CommitRequest.Origin.CLIENT)))));
            assertEquals(0, coordinator.submissions.get());
            var next = executor.execute(command("good", "1_7", CommitRequest.Origin.CLIENT));
            coordinator.take().committed(true); result(next);
        } finally { drain(executor); }
    }

    @Test void epochChangesDuringPreparationRejectOldDecisionWithoutReplacingItsToken() throws Exception {
        var coordinator = new Coordinator();
        var preparing = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var executor = executor(coordinator, new Handler() {
            @Override public CommitDecision prepare(OperationCommand command) throws Exception {
                preparing.countDown(); await(release); return decision(command);
            }
        });
        try {
            var handle = executor.execute(command("first", "1_7", CommitRequest.Origin.CLIENT));
            await(preparing);
            coordinator.writer.set(new WriterToken(REALM, WRITER.instanceId(), 2));
            release.countDown();
            assertEquals(CommitResult.Reason.OWNER_LOST, rejected(result(handle)));
            assertEquals(0, coordinator.submissions.get());
        } finally { release.countDown(); drain(executor); }
    }

    @Test void systemReservationsApplyAtOperationAdmissionAndToActualPayloadBytes() throws Exception {
        var coordinator = new Coordinator();
        var limits = new OperationLimits(2, 100_000, 10, 2, 1, 1, 1, 0, Duration.ofSeconds(5));
        var executor = executor(coordinator, new Handler(), limits, request -> 10);
        try {
            var first = executor.execute(command("client", "1_7", CommitRequest.Origin.CLIENT));
            var clientCommit = coordinator.take();
            assertEquals(CommitResult.Reason.QUEUE_FULL, rejected(result(executor.execute(command("blocked", "1_8", CommitRequest.Origin.CLIENT)))));
            var system = executor.execute(command("system", "1_8", CommitRequest.Origin.DUE_TASK));
            var systemCommit = coordinator.take();
            clientCommit.committed(true); systemCommit.committed(true); result(first); result(system);
        } finally { drain(executor); }
        var byteCoordinator = new Coordinator();
        var bytes = executor(byteCoordinator, new Handler(), new OperationLimits(4, 1000, 10, 2, 1, 1, 1, 600, Duration.ofSeconds(5)), request -> 10);
        try {
            var client = command("large", "1_7", CommitRequest.Origin.CLIENT);
            assertEquals(CommitResult.Reason.QUEUE_FULL, rejected(result(bytes.execute(new OperationCommand(client.scope(), "TYPE", new byte[300])))));
            var system = command("large", "1_7", CommitRequest.Origin.DUE_TASK);
            var accepted = bytes.execute(new OperationCommand(system.scope(), "TYPE", new byte[300]));
            byteCoordinator.take().committed(true); result(accepted);
        } finally { drain(bytes); }
    }

    @Test void preparedDecisionMustFitAdditionalRetentionBudgetBeforeStorageSubmission() throws Exception {
        var coordinator = new Coordinator();
        var executor = executor(coordinator, new Handler(), new OperationLimits(4, 1000, 10, 2, 1, 1, 0, 0, Duration.ofSeconds(5)), request -> 999);
        try {
            assertEquals(CommitResult.Reason.QUEUE_FULL,
                    rejected(result(executor.execute(command("large", "1_7", CommitRequest.Origin.CLIENT)))));
            assertEquals(0, coordinator.submissions.get());
            await(() -> executor.stats().retainedBytes() == 0);
        } finally { drain(executor); }
    }

    @Test void blockedClientNotificationKeepsCapacityChargedWithoutBlockingQualificationRelease() throws Exception {
        var coordinator = new Coordinator();
        var executor = executor(coordinator, new Handler(), new OperationLimits(2, 100_000, 10, 2, 1, 1, 0, 0, Duration.ofSeconds(5)), request -> 10);
        var notificationEntered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try {
            var first = executor.execute(command("first", "1_7", CommitRequest.Origin.CLIENT));
            first.completion().thenRun(() -> { notificationEntered.countDown(); uncheckedAwait(release); });
            coordinator.take().committed(true);
            await(notificationEntered);
            var second = executor.execute(command("second", "1_7", CommitRequest.Origin.CLIENT));
            coordinator.take().committed(true);
            await(() -> executor.stats().notifications() == 2);
            assertEquals(CommitResult.Reason.QUEUE_FULL,
                    rejected(result(executor.execute(command("third", "1_8", CommitRequest.Origin.CLIENT)))));
            assertFalse(executor.drain().toCompletableFuture().isDone());
            release.countDown(); result(first); result(second);
        } finally { release.countDown(); drain(executor); }
    }

    @Test void waitingTimeoutAndDrainRejectionCannotStartBusinessPreparation() throws Exception {
        var coordinator = new Coordinator();
        var executor = executor(coordinator, new Handler());
        var first = executor.execute(command("first", "1_7", CommitRequest.Origin.CLIENT));
        var commit = coordinator.take();
        var timed = command("timeout", "1_7", CommitRequest.Origin.CLIENT);
        var scope = new OperationScope(timed.scope().request(), timed.scope().objects(), timed.scope().origin(),
                Duration.ofMillis(30), Duration.ofSeconds(1));
        assertEquals(CommitResult.Reason.QUEUE_TIMEOUT, rejected(result(executor.execute(new OperationCommand(scope, "TYPE", new byte[0])))));
        var draining = executor.drain().toCompletableFuture();
        assertEquals(CommitResult.Reason.STOPPING, rejected(result(executor.execute(command("stopped", "1_8", CommitRequest.Origin.CLIENT)))));
        assertFalse(draining.isDone());
        commit.rejected(CommitResult.Reason.ROLLED_BACK); result(first); draining.get(3, TimeUnit.SECONDS);
        assertEquals(1, coordinator.submissions.get());
    }

    @Test void globalMaximumWaitBoundsEvenAnExtremelyLargePerRequestBudget() throws Exception {
        var coordinator = new Coordinator();
        var executor = executor(coordinator, new Handler(),
                new OperationLimits(4, 100_000, 10, 2, 1, 1, 0, 0, Duration.ofMillis(30)), request -> 10);
        var first = executor.execute(command("first", "1_7", CommitRequest.Origin.CLIENT));
        var commit = coordinator.take();
        try {
            var command = command("long-wait", "1_7", CommitRequest.Origin.CLIENT);
            var scope = new OperationScope(command.scope().request(), command.scope().objects(), command.scope().origin(),
                    Duration.ofDays(1000), Duration.ofSeconds(1));
            assertEquals(CommitResult.Reason.QUEUE_TIMEOUT,
                    rejected(result(executor.execute(new OperationCommand(scope, "TYPE", new byte[0])))));
            assertEquals(1, coordinator.submissions.get());
        } finally { commit.rejected(CommitResult.Reason.ROLLED_BACK); result(first); drain(executor); }
    }

    private static BoundedOperationExecutor executor(Coordinator coordinator, Handler handler) {
        return executor(coordinator, handler, new OperationLimits(16, 1_000_000, 32, 2, 2, 2, 2, 1000, Duration.ofSeconds(5)), request -> 100);
    }
    private static BoundedOperationExecutor executor(Coordinator coordinator, Handler handler, OperationLimits limits, CommitPayloadSizer sizer) {
        return new BoundedOperationExecutor(REALM, coordinator, sizer, Map.of("TYPE", handler), limits);
    }
    private static OperationCommand command(String id, String player, CommitRequest.Origin origin) {
        return new OperationCommand(new OperationScope(new RequestKey("scope", id), Set.of(new ObjectKey.Player(player)),
                origin, Duration.ofSeconds(5), Duration.ofSeconds(5)), "TYPE", new byte[0]);
    }
    private static OperationCommand troop(String id, TroopType type) {
        return new OperationCommand(new OperationScope(new RequestKey("scope", id),
                Set.of(new ObjectKey.Player("1_7"), new ObjectKey.Troop("1_7", type)), CommitRequest.Origin.CLIENT,
                Duration.ofSeconds(5), Duration.ofSeconds(5)), "TYPE", new byte[0]);
    }
    private static CommitDecision decision(OperationCommand command) {
        return new CommitDecision(command.scope().request(), "digest", "FACT", "{}", "{}", List.of(), Map.of());
    }
    private static CommitResult result(OperationHandle handle) throws Exception { return handle.completion().toCompletableFuture().get(3, TimeUnit.SECONDS); }
    private static CommitResult.Reason rejected(CommitResult result) { return assertInstanceOf(CommitResult.NotCommitted.class, result).reason(); }
    private static void drain(BoundedOperationExecutor executor) throws Exception { executor.drain().toCompletableFuture().get(3, TimeUnit.SECONDS); }
    private static void await(CountDownLatch latch) throws InterruptedException { assertTrue(latch.await(3, TimeUnit.SECONDS)); }
    private static void uncheckedAwait(CountDownLatch latch) {
        try { await(latch); } catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
    }
    private static void await(BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        assertTrue(condition.getAsBoolean());
    }
    private static OperationPhase eventuallySubmitted(Coordinator coordinator, OperationHandle handle, String request) throws Exception {
        var commit = coordinator.take();
        assertEquals(request, commit.request.request().requestId());
        var phase = handle.phase();
        commit.committed(true);
        return phase;
    }

    private static class Handler implements OperationHandler {
        @Override public CommitDecision prepare(OperationCommand command) throws Exception { return decision(command); }
        @Override public void publish(OperationCommand command, CommitDecision decision, CommitResult.Committed result) throws Exception {}
        @Override public void reload(OperationCommand command, CommitDecision decision, CommitResult.Committed result) throws Exception {}
    }
    private static final class Coordinator implements CommitCoordinator {
        final AtomicReference<WriterToken> writer = new AtomicReference<>(WRITER);
        final AtomicInteger barriers = new AtomicInteger();
        final AtomicInteger submissions = new AtomicInteger();
        final BlockingQueue<Attempt> queue = new LinkedBlockingQueue<>();
        @Override public Optional<WriterToken> currentWriter() { return barriers.get() == 0 ? Optional.of(writer.get()) : Optional.empty(); }
        @Override public PublicationBarrier isolatePublication() {
            barriers.incrementAndGet();
            var released = new AtomicBoolean();
            return () -> { if (released.compareAndSet(false, true)) barriers.decrementAndGet(); };
        }
        @Override public CommitHandle submit(CommitRequest request) {
            var attempt = new Attempt(request.decision());
            submissions.incrementAndGet(); queue.add(attempt); return attempt;
        }
        @Override public CompletionStage<Void> drain() { return CompletableFuture.completedFuture(null); }
        Attempt take() throws InterruptedException { return Objects.requireNonNull(queue.poll(3, TimeUnit.SECONDS), "Expected submission"); }
    }
    private static final class Attempt implements CommitHandle {
        final CommitDecision request;
        final CompletableFuture<CommitResult> completion = new CompletableFuture<>();
        volatile CommitPhase phase = CommitPhase.EXECUTING;
        Attempt(CommitDecision request) { this.request = request; }
        @Override public RequestKey requestKey() { return request.request(); }
        @Override public CommitPhase phase() { return phase; }
        @Override public CompletionStage<CommitResult> settlement() { return completion.minimalCompletionStage(); }
        @Override public boolean cancelBeforeStart() { return false; }
        void committed(boolean applied) {
            phase = CommitPhase.SETTLED;
            completion.complete(new CommitResult.Committed(new CommitReceipt(request.request(), 1, request.digest(), request.resultJson()), applied));
        }
        void rejected(CommitResult.Reason reason) { phase = CommitPhase.SETTLED; completion.complete(new CommitResult.NotCommitted(reason)); }
    }
}
