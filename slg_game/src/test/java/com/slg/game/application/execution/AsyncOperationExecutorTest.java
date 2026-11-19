package com.slg.game.application.execution;

import com.slg.game.application.commit.*;
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

/** 故障/阻塞点放在真实阶段边界，证明等待释放线程但不会提前释放账号资格或伪造提交结果。 */
@Timeout(10)
class AsyncOperationExecutorTest {
    private static final RealmIdentity REALM = new RealmIdentity(1, UUID.randomUUID());
    private static final WriterToken WRITER = new WriterToken(REALM, UUID.randomUUID(), 1);

    @Test void coldPreparationReleasesOnlyWorkerAndSerializesDifferentFoundingRealmsOfSameAccount() throws Exception {
        var coordinator = new Coordinator();
        var cold = new CompletableFuture<CommitDecision>();
        var entered = new CountDownLatch(1);
        var handler = new Handler() {
            @Override public CompletionStage<CommitDecision> prepareAsync(OperationCommand command) {
                if (command.scope().request().requestId().equals("cold")) { entered.countDown(); return cold; }
                return CompletableFuture.completedFuture(decision(command));
            }
        };
        var executor = executor(coordinator, handler, Map.of());
        var firstCommand = write("cold", "1_7");
        try {
            var first = executor.execute(firstCommand);
            await(entered);
            var sameAccount = executor.execute(write("same-account", "2_7"));
            var independent = executor.execute(write("independent", "1_8"));
            assertEquals("independent", coordinator.take().commit(true));
            result(independent);
            assertEquals(OperationPhase.WAITING, sameAccount.phase());
            assertEquals(OperationPhase.PREPARING, first.phase());
            cold.complete(decision(firstCommand));
            assertEquals("cold", coordinator.take().commit(true));
            result(first);
            assertEquals("same-account", coordinator.take().commit(true));
            result(sameAccount);
        } finally { cold.complete(decision(firstCommand)); close(executor, coordinator); }
    }

    @Test void asynchronousPublicationDoesNotOccupyTheOnlyPublicationWorkerOrReleaseItsScope() throws Exception {
        var coordinator = new Coordinator();
        var publication = new CompletableFuture<Void>();
        var entered = new CountDownLatch(1);
        var executor = executor(coordinator, new Handler() {
            @Override public CompletionStage<Void> publishAsync(OperationCommand command, CommitDecision decision, CommitResult.Committed result) {
                if (command.scope().request().requestId().equals("first")) { entered.countDown(); return publication; }
                return CompletableFuture.completedFuture(null);
            }
        }, Map.of());
        try {
            var first = executor.execute(write("first", "1_7"));
            coordinator.take().commit(true); await(entered);
            var waiting = executor.execute(write("waiting", "2_7"));
            var other = executor.execute(write("other", "1_8"));
            coordinator.take().commit(true); result(other);
            assertEquals(OperationPhase.WAITING, waiting.phase());
            assertFalse(first.completion().toCompletableFuture().isDone());
            publication.complete(null); result(first);
            coordinator.take().commit(true); result(waiting);
        } finally { publication.complete(null); close(executor, coordinator); }
    }

    @Test void asynchronousReplayReloadFailureRetainsBarrierAndExplicitRetryUsesReloadAgain() throws Exception {
        var coordinator = new Coordinator();
        var reload = new CompletableFuture<Void>();
        var reloads = new AtomicInteger();
        var executor = executor(coordinator, new Handler() {
            @Override public CompletionStage<Void> reloadAsync(OperationCommand command, CommitDecision decision, CommitResult.Committed result) {
                return reloads.incrementAndGet() == 1 ? reload : CompletableFuture.completedFuture(null);
            }
        }, Map.of());
        try {
            var operation = executor.execute(write("replay", "1_7"));
            coordinator.take().commit(false);
            await(() -> reloads.get() == 1);
            reload.completeExceptionally(new IllegalStateException("read failed"));
            await(() -> operation.phase() == OperationPhase.ISOLATED);
            assertEquals(1, coordinator.barriers.get());
            assertFalse(executor.drain().toCompletableFuture().isDone());
            assertTrue(executor.retryRepair(operation.requestKey()));
            assertFalse(assertInstanceOf(CommitResult.Committed.class, result(operation)).applied());
            assertEquals(2, reloads.get());
            assertEquals(0, coordinator.barriers.get());
        } finally { reload.complete(null); close(executor, coordinator); }
    }

    @Test void queryAndWriteShareAccountFifoWithoutSubmittingQueryOrRetainingAWorker() throws Exception {
        var coordinator = new Coordinator();
        var queryStage = new CompletableFuture<OperationValue>();
        var entered = new CountDownLatch(1);
        var queryHandler = query(8, command -> {
            if (command.scope().request().requestId().equals("open")) { entered.countDown(); return queryStage; }
            return CompletableFuture.completedFuture(new OperationValue(new byte[]{9}));
        });
        var executor = executor(coordinator, new Handler(), Map.of("QUERY", queryHandler));
        try {
            var first = executor.execute(write("write", "2_7"));
            var committed = coordinator.take();
            var open = executor.executeQuery(queryCommand("open", 7));
            assertEquals(OperationPhase.WAITING, open.phase());
            committed.commit(true); result(first); await(entered);
            var successor = executor.execute(write("after-open", "1_7"));
            var independent = executor.executeQuery(queryCommand("independent", 8));
            assertArrayEquals(new byte[]{9}, value(independent).bytes());
            assertEquals(OperationPhase.WAITING, successor.phase());
            assertEquals(1, coordinator.submissions.get());
            queryStage.complete(new OperationValue(new byte[]{1}));
            assertArrayEquals(new byte[]{1}, value(open).bytes());
            assertEquals("after-open", coordinator.take().commit(true));
            result(successor);
            assertEquals(2, coordinator.submissions.get());
        } finally { queryStage.complete(new OperationValue(new byte[0])); close(executor, coordinator); }
    }

    @Test void cancellationOfQueryViewCannotReleaseRunningQualificationButWaitingCancellationCan() throws Exception {
        var coordinator = new Coordinator();
        var stage = new CompletableFuture<OperationValue>();
        var entered = new CountDownLatch(1);
        var executor = executor(coordinator, new Handler(), Map.of("QUERY", query(0, command -> {
            entered.countDown(); return stage;
        })));
        try {
            var running = executor.executeQuery(queryCommand("running", 7)); await(entered);
            running.result().toCompletableFuture().cancel(true);
            assertFalse(running.cancelBeforeStart());
            var cancelled = executor.executeQuery(queryCommand("cancelled", 7));
            assertTrue(cancelled.cancelBeforeStart());
            assertEquals(CommitResult.Reason.CANCELLED, queryRejected(cancelled));
            var write = executor.execute(write("write", "2_7"));
            assertEquals(OperationPhase.WAITING, write.phase());
            assertEquals(0, coordinator.submissions.get());
            stage.complete(new OperationValue(new byte[0])); value(running);
            coordinator.take().commit(true); result(write);
        } finally { stage.complete(new OperationValue(new byte[0])); close(executor, coordinator); }
    }

    @Test void queryFailureAndOversizedResultsAreExceptionsWithoutCommitConclusions() throws Exception {
        var coordinator = new Coordinator();
        var failure = new IllegalStateException("cold-read failed");
        var executor = executor(coordinator, new Handler(), Map.of("QUERY", query(1, command ->
                command.scope().request().requestId().equals("failure") ? CompletableFuture.failedFuture(failure)
                        : CompletableFuture.completedFuture(new OperationValue(new byte[2])))));
        try {
            assertSame(failure, queryFailure(executor.executeQuery(queryCommand("failure", 7))));
            assertInstanceOf(IllegalArgumentException.class, queryFailure(executor.executeQuery(queryCommand("large", 7))));
            assertEquals(0, coordinator.submissions.get());
        } finally { close(executor, coordinator); }
    }

    @Test void resultMaximumIsReservedAtAdmissionAndNotificationRetainsCapacity() throws Exception {
        var coordinator = new Coordinator();
        var stage = new CompletableFuture<OperationValue>();
        var notification = new CountDownLatch(1);
        var unblock = new CountDownLatch(1);
        var limits = new OperationLimits(4, 1400, 16, 1, 1, 1, 0, 0, Duration.ofSeconds(3));
        var executor = new BoundedOperationExecutor(REALM, coordinator, request -> 1, Map.of("WRITE", new Handler()), limits,
                Map.of("QUERY", query(800, command -> stage)));
        try {
            var first = executor.executeQuery(queryCommand("first", 7));
            first.result().thenRun(() -> { notification.countDown(); uncheckedAwait(unblock); });
            assertEquals(CommitResult.Reason.QUEUE_FULL, queryRejected(executor.executeQuery(queryCommand("full", 8))));
            stage.complete(new OperationValue(new byte[1])); await(notification);
            assertTrue(executor.stats().retainedBytes() >= 800);
            assertEquals(CommitResult.Reason.QUEUE_FULL, queryRejected(executor.executeQuery(queryCommand("still-full", 8))));
            assertFalse(executor.drain().toCompletableFuture().isDone());
            unblock.countDown(); value(first);
        } finally { stage.complete(new OperationValue(new byte[0])); unblock.countDown(); close(executor, coordinator); }
    }

    @Test void queryWaitingTimeoutAndSameRequestAcrossAccountsUseTheCommonQueues() throws Exception {
        var coordinator = new Coordinator();
        var stage = new CompletableFuture<OperationValue>();
        var entered = new CountDownLatch(1);
        var executor = executor(coordinator, new Handler(), Map.of("QUERY", query(0, command -> {
            entered.countDown(); return stage;
        })));
        try {
            var first = executor.executeQuery(queryCommand("same", 7)); await(entered);
            var other = queryCommand("same", 8);
            var scope = new OperationScope(first.requestKey(), other.scope().objects(), CommitRequest.Origin.ADMIN,
                    Duration.ofMillis(30), Duration.ZERO, other.scope().accounts());
            assertEquals(CommitResult.Reason.QUEUE_TIMEOUT,
                    queryRejected(executor.executeQuery(new OperationCommand(scope, "QUERY", new byte[0]))));
            stage.complete(new OperationValue(new byte[0])); value(first);
            assertEquals(0, coordinator.submissions.get());
        } finally { stage.complete(new OperationValue(new byte[0])); close(executor, coordinator); }
    }

    @Test void queryResultTakesDefensiveCopies() {
        byte[] source = {1, 2};
        var value = new OperationValue(source);
        source[0] = 9; value.bytes()[1] = 9;
        assertArrayEquals(new byte[]{1, 2}, value.bytes());
        assertEquals(2, value.size());
    }

    @Test void notCommittedDiscardMustFinishWithinQualificationBeforeTheSuccessorPrepares() throws Exception {
        var coordinator = new Coordinator();
        var discard = new CompletableFuture<Void>();
        var entered = new CountDownLatch(1);
        var executor = executor(coordinator, new Handler() {
            @Override public CompletionStage<Void> discardAsync(OperationCommand command, CommitResult.NotCommitted rejected) {
                entered.countDown(); return discard;
            }
        }, Map.of());
        try {
            var first = executor.execute(write("conflict", "1_7"));
            coordinator.take().reject(CommitResult.Reason.VERSION_CONFLICT); await(entered);
            var successor = executor.execute(write("successor", "2_7"));
            assertEquals(OperationPhase.WAITING, successor.phase());
            assertFalse(first.completion().toCompletableFuture().isDone());
            discard.complete(null);
            assertEquals(CommitResult.Reason.VERSION_CONFLICT, assertInstanceOf(CommitResult.NotCommitted.class, result(first)).reason());
            coordinator.take().commit(true); result(successor);
        } finally { discard.complete(null); close(executor, coordinator); }
    }

    @Test void failedDiscardKeepsKnownRejectionIsolatedUntilExplicitRepair() throws Exception {
        var coordinator = new Coordinator();
        var attempts = new AtomicInteger();
        var executor = executor(coordinator, new Handler() {
            @Override public CompletionStage<Void> discardAsync(OperationCommand command, CommitResult.NotCommitted rejected) {
                return attempts.incrementAndGet() == 1 ? CompletableFuture.failedFuture(new IllegalStateException("invalidate failed"))
                        : CompletableFuture.completedFuture(null);
            }
        }, Map.of());
        try {
            var first = executor.execute(write("conflict", "1_7"));
            coordinator.take().reject(CommitResult.Reason.VERSION_CONFLICT);
            await(() -> first.phase() == OperationPhase.ISOLATED);
            assertEquals(1, coordinator.barriers.get());
            assertFalse(executor.drain().toCompletableFuture().isDone());
            assertTrue(executor.retryRepair(first.requestKey()));
            assertEquals(CommitResult.Reason.VERSION_CONFLICT, assertInstanceOf(CommitResult.NotCommitted.class, result(first)).reason());
            assertEquals(0, coordinator.barriers.get());
            assertEquals(2, attempts.get());
        } finally { close(executor, coordinator); }
    }

    @Test void failedPreparationDoesNotDiscardButChangedWriterAfterPreparationDoes() throws Exception {
        var coordinator = new Coordinator();
        var stage = new CompletableFuture<CommitDecision>();
        var entered = new CountDownLatch(1);
        var discards = new AtomicInteger();
        var executor = executor(coordinator, new Handler() {
            @Override public CompletionStage<CommitDecision> prepareAsync(OperationCommand command) {
                if (command.scope().request().requestId().equals("failure")) return CompletableFuture.failedFuture(new IllegalStateException());
                entered.countDown(); return stage;
            }
            @Override public CompletionStage<Void> discardAsync(OperationCommand command, CommitResult.NotCommitted rejected) {
                discards.incrementAndGet(); return CompletableFuture.completedFuture(null);
            }
        }, Map.of());
        var changing = write("owner", "1_7");
        try {
            assertEquals(CommitResult.Reason.INVALID_DECISION, assertInstanceOf(CommitResult.NotCommitted.class,
                    result(executor.execute(write("failure", "1_7")))).reason());
            assertEquals(0, discards.get());
            var handle = executor.execute(changing); await(entered);
            coordinator.writer.set(new WriterToken(REALM, WRITER.instanceId(), 2));
            stage.complete(decision(changing));
            assertEquals(CommitResult.Reason.OWNER_LOST, assertInstanceOf(CommitResult.NotCommitted.class, result(handle)).reason());
            assertEquals(1, discards.get());
            assertEquals(0, coordinator.submissions.get());
        } finally { stage.complete(decision(changing)); close(executor, coordinator); }
    }

    private static BoundedOperationExecutor executor(Coordinator coordinator, OperationHandler handler,
                                                      Map<String, QualifiedQueryHandler> queries) {
        return new BoundedOperationExecutor(REALM, coordinator, request -> 64, Map.of("WRITE", handler),
                new OperationLimits(16, 100_000, 16, 1, 1, 1, 1, 1000, Duration.ofSeconds(3)), queries);
    }
    private static OperationCommand write(String request, String player) {
        return new OperationCommand(new OperationScope(new RequestKey("write", request), Set.of(new ObjectKey.Player(player)),
                CommitRequest.Origin.CLIENT, Duration.ofSeconds(3), Duration.ofSeconds(3)), "WRITE", new byte[0]);
    }
    private static OperationCommand queryCommand(String request, long account) {
        return new OperationCommand(new OperationScope(new RequestKey("account:" + account, request), Set.of(),
                CommitRequest.Origin.CLIENT, Duration.ofSeconds(3), Duration.ZERO, Set.of(account)), "QUERY", new byte[0]);
    }
    private interface QueryBody { CompletionStage<OperationValue> run(OperationCommand command) throws Exception; }
    private static QualifiedQueryHandler query(int maximum, QueryBody body) {
        return new QualifiedQueryHandler() {
            @Override public CompletionStage<OperationValue> execute(OperationCommand command) throws Exception { return body.run(command); }
            @Override public int maxResultBytes() { return maximum; }
        };
    }
    private static CommitDecision decision(OperationCommand command) {
        return new CommitDecision(command.scope().request(), "digest", "FACT", "{}", "{}", List.of(), Map.of());
    }
    private static class Handler implements OperationHandler {
        @Override public CommitDecision prepare(OperationCommand command) { return decision(command); }
        @Override public void publish(OperationCommand command, CommitDecision decision, CommitResult.Committed result) {}
        @Override public void reload(OperationCommand command, CommitDecision decision, CommitResult.Committed result) {}
    }
    private static final class Coordinator implements CommitCoordinator {
        final AtomicReference<WriterToken> writer = new AtomicReference<>(WRITER);
        final AtomicInteger barriers = new AtomicInteger();
        final AtomicInteger submissions = new AtomicInteger();
        final BlockingQueue<Attempt> queue = new LinkedBlockingQueue<>();
        final List<Attempt> attempts = new CopyOnWriteArrayList<>();
        @Override public Optional<WriterToken> currentWriter() { return barriers.get() == 0 ? Optional.of(writer.get()) : Optional.empty(); }
        @Override public PublicationBarrier isolatePublication() {
            barriers.incrementAndGet(); var released = new AtomicBoolean();
            return () -> { if (released.compareAndSet(false, true)) barriers.decrementAndGet(); };
        }
        @Override public CommitHandle submit(CommitRequest request) {
            var attempt = new Attempt(request.decision()); submissions.incrementAndGet(); attempts.add(attempt); queue.add(attempt); return attempt;
        }
        @Override public CompletionStage<Void> drain() { return CompletableFuture.completedFuture(null); }
        Attempt take() throws InterruptedException { return Objects.requireNonNull(queue.poll(3, TimeUnit.SECONDS), "Expected submission"); }
    }
    private static final class Attempt implements CommitHandle {
        final CommitDecision decision;
        final CompletableFuture<CommitResult> completion = new CompletableFuture<>();
        Attempt(CommitDecision decision) { this.decision = decision; }
        @Override public RequestKey requestKey() { return decision.request(); }
        @Override public CommitPhase phase() { return completion.isDone() ? CommitPhase.SETTLED : CommitPhase.EXECUTING; }
        @Override public CompletionStage<CommitResult> settlement() { return completion.minimalCompletionStage(); }
        @Override public boolean cancelBeforeStart() { return false; }
        String commit(boolean applied) {
            completion.complete(new CommitResult.Committed(new CommitReceipt(decision.request(), 1, decision.digest(), decision.resultJson()), applied));
            return decision.request().requestId();
        }
        void reject(CommitResult.Reason reason) { completion.complete(new CommitResult.NotCommitted(reason)); }
    }
    private static CommitResult result(OperationHandle handle) throws Exception { return handle.completion().toCompletableFuture().get(3, TimeUnit.SECONDS); }
    private static OperationValue value(QueryHandle handle) throws Exception { return handle.result().toCompletableFuture().get(3, TimeUnit.SECONDS); }
    private static Throwable queryFailure(QueryHandle handle) {
        return assertThrows(ExecutionException.class, () -> value(handle)).getCause();
    }
    private static CommitResult.Reason queryRejected(QueryHandle handle) { return assertInstanceOf(QueryRejectedException.class, queryFailure(handle)).reason(); }
    private static void close(BoundedOperationExecutor executor, Coordinator coordinator) throws Exception {
        var drained = executor.drain().toCompletableFuture();
        coordinator.attempts.forEach(attempt -> attempt.reject(CommitResult.Reason.ROLLED_BACK));
        drained.get(3, TimeUnit.SECONDS);
    }
    private static void await(CountDownLatch latch) throws InterruptedException { assertTrue(latch.await(3, TimeUnit.SECONDS)); }
    private static void uncheckedAwait(CountDownLatch latch) {
        try { await(latch); } catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
    }
    private static void await(BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        assertTrue(condition.getAsBoolean());
    }
}
