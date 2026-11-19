package com.slg.game.application.read;

import com.slg.game.application.port.ReadBudget;
import com.slg.game.application.port.RealmStateReader;
import com.slg.game.application.port.RealmReceiptReader;
import com.slg.game.domain.persistence.ObjectKey;
import com.slg.game.domain.persistence.ObjectKind;
import com.slg.game.domain.persistence.RealmIdentity;
import com.slg.game.domain.persistence.RequestKey;
import com.slg.game.domain.persistence.VersionedState;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BoundedReadExecutorTest {
    private static final RealmIdentity IDENTITY = new RealmIdentity(1, UUID.randomUUID());
    private static final ReadBudget BUDGET = new ReadBudget(100, 4096, Duration.ofSeconds(5));

    @Test void boundsReadConcurrencyAndRejectsBeforeOpeningAnotherSnapshot() throws Exception {
        var reader = new RecordingReader();
        var entered = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        try (var executor = executor(2, 3)) {
            var handles = new ArrayList<ReadHandle<Long>>();
            for (int i = 0; i < 3; i++) handles.add(executor.readSnapshot(reader, IDENTITY, BUDGET, view -> {
                entered.countDown(); await(release); return view.highSequence();
            }));
            await(entered);
            assertEquals(2, reader.opened.get());
            assertEquals(2, executor.statistics().executing());
            assertEquals(1, executor.statistics().queued());
            assertEquals(ReadRejectedException.Reason.CAPACITY, assertThrows(ReadRejectedException.class,
                    () -> executor.readSnapshot(reader, IDENTITY, BUDGET, SnapshotView::highSequence)).reason());
            release.countDown();
            for (var handle : handles) assertEquals(42L, handle.result().toCompletableFuture().get(2, TimeUnit.SECONDS));
            assertTrue(executor.awaitDrain(Duration.ofSeconds(2)));
            assertEquals(3, reader.closed.get());
            assertEquals(0, executor.statistics().outstanding());
        } finally { release.countDown(); }
    }

    @Test void metadataBudgetIsMeasuredInternallyAndIndependentFromRowBudget() throws Exception {
        var release = new CountDownLatch(1);
        var reader = new RecordingReader();
        var limits = new ReadExecutionLimits(1, 3, BoundedReadExecutor.TASK_METADATA_BYTES,
                1, 2, Duration.ofSeconds(2));
        try (var executor = new BoundedReadExecutor(limits)) {
            var handle = executor.readSnapshot(reader, IDENTITY, BUDGET, view -> { await(release); return 1; });
            assertEquals(BoundedReadExecutor.TASK_METADATA_BYTES, executor.statistics().metadataBytes());
            assertThrows(ReadRejectedException.class, () -> executor.readSnapshot(reader, IDENTITY,
                    new ReadBudget(1, 1, Duration.ofMillis(1)), view -> 2));
            release.countDown();
            assertEquals(1, handle.result().toCompletableFuture().get(2, TimeUnit.SECONDS));
        } finally { release.countDown(); }
    }

    @Test void receiptKeysChargeTheirActualAsciiBytesAndUseTheSameAdmissionAndNotifier() throws Exception {
        var key = new RequestKey("s".repeat(256), "r".repeat(128));
        long charge = BoundedReadExecutor.TASK_METADATA_BYTES + 384;
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var worker = new AtomicReference<String>();
        var notifier = new AtomicReference<String>();
        RealmReceiptReader receiptReader = (identity, request, budget) -> {
            assertEquals(IDENTITY, identity); assertEquals(key, request); assertSame(BUDGET, budget);
            worker.set(Thread.currentThread().getName()); entered.countDown(); awaitUnchecked(release);
            return new RealmReceiptReader.Observation.NotObserved();
        };
        var limits = new ReadExecutionLimits(1, 2, charge, 1, 1, Duration.ofSeconds(2));
        try (var executor = new BoundedReadExecutor(limits)) {
            var receipt = executor.readReceipt(receiptReader, IDENTITY, key, BUDGET);
            var notified = receipt.result().thenRun(() -> notifier.set(Thread.currentThread().getName()));
            await(entered);
            assertEquals(charge, executor.statistics().metadataBytes());
            assertThrows(ReadRejectedException.class,
                    () -> executor.readSnapshot(new RecordingReader(), IDENTITY, BUDGET, SnapshotView::highSequence));
            assertTrue(receipt.result().toCompletableFuture().cancel(true));
            assertFalse(receipt.cancelBeforeStart());
            assertEquals(charge, executor.statistics().metadataBytes());
            release.countDown();
            notified.toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertInstanceOf(RealmReceiptReader.Observation.NotObserved.class, receipt.result().toCompletableFuture().join());
            assertTrue(worker.get().contains("-worker-"));
            assertTrue(notifier.get().contains("-notify-"));
            assertTrue(executor.awaitDrain(Duration.ofSeconds(2)));
            assertEquals(0, executor.statistics().metadataBytes());
        } finally { release.countDown(); }
        var insufficient = new ReadExecutionLimits(1, 1, charge - 1, 1, 0, Duration.ofSeconds(2));
        try (var executor = new BoundedReadExecutor(insufficient)) {
            assertEquals(ReadRejectedException.Reason.CAPACITY, assertThrows(ReadRejectedException.class,
                    () -> executor.readReceipt(receiptReader, IDENTITY, key, BUDGET)).reason());
            assertEquals(0, executor.statistics().outstanding());
        }
    }

    @Test void cancellingDerivedFutureDoesNotCancelRunningReadOrReleaseItsAdmission() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var reader = new RecordingReader();
        try (var executor = executor(1, 1)) {
            var handle = executor.readSnapshot(reader, IDENTITY, BUDGET, view -> {
                entered.countDown(); await(release); return view.highSequence();
            });
            await(entered);
            assertTrue(handle.result().toCompletableFuture().cancel(true));
            assertFalse(handle.cancelBeforeStart());
            assertEquals(1, executor.statistics().outstanding());
            assertThrows(ReadRejectedException.class,
                    () -> executor.readSnapshot(reader, IDENTITY, BUDGET, SnapshotView::highSequence));
            release.countDown();
            assertEquals(42L, handle.result().toCompletableFuture().get(2, TimeUnit.SECONDS));
            assertEquals(1, reader.closed.get());
        } finally { release.countDown(); }
    }

    @Test void queuedCancellationNeverOpensSnapshotAndDrainWaitsForRunningWork() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var reader = new RecordingReader();
        try (var executor = executor(1, 3)) {
            var running = executor.readSnapshot(reader, IDENTITY, BUDGET, view -> {
                entered.countDown(); await(release); return 1;
            });
            await(entered);
            var cancelled = executor.readSnapshot(reader, IDENTITY, BUDGET, view -> 2);
            var cancelledByDrain = executor.readSnapshot(reader, IDENTITY, BUDGET, view -> 3);
            assertTrue(cancelled.cancelBeforeStart());
            assertFalse(cancelled.cancelBeforeStart());
            var drained = executor.drain().toCompletableFuture();
            assertFalse(drained.isDone());
            assertFalse(executor.awaitDrain(Duration.ofMillis(20)));
            assertEquals(ReadRejectedException.Reason.STOPPED, assertThrows(ReadRejectedException.class,
                    () -> executor.readSnapshot(reader, IDENTITY, BUDGET, view -> 4)).reason());
            assertEquals(3, executor.statistics().outstanding());
            release.countDown();
            assertEquals(1, running.result().toCompletableFuture().get(2, TimeUnit.SECONDS));
            assertThrows(CompletionException.class, () -> cancelled.result().toCompletableFuture().join());
            assertThrows(CompletionException.class, () -> cancelledByDrain.result().toCompletableFuture().join());
            drained.get(2, TimeUnit.SECONDS);
            assertEquals(1, reader.opened.get());
            assertEquals(1, reader.closed.get());
        } finally { release.countDown(); }
    }

    @Test void fullSnapshotUsesOneWorkerThenNotifiesSeparatelyAndInvalidatesEscapedViews() throws Exception {
        var reader = new RecordingReader();
        var captured = new AtomicReference<SnapshotView>();
        var release = new CountDownLatch(1);
        var notificationThread = new AtomicReference<String>();
        try (var executor = executor(1, 2)) {
            var handle = executor.readSnapshot(reader, IDENTITY, BUDGET, view -> {
                captured.set(view);
                assertEquals(IDENTITY, view.identity());
                view.find(new ObjectKey.Player("1_1"));
                view.scan(ObjectKind.PLAYER, ignored -> { });
                view.scanOwned(ObjectKind.RESOURCE, "1_1", ignored -> { });
                view.scanActiveMarches("1_1", ignored -> { });
                await(release);
                return List.of(view.highSequence());
            });
            var notified = handle.result().thenAccept(value -> {
                notificationThread.set(Thread.currentThread().getName());
                assertEquals(1, reader.closed.get(), "Result must not become visible before snapshot close");
            });
            release.countDown();
            notified.toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertTrue(notificationThread.get().contains("-notify-"));
            assertTrue(reader.lastThread.get().contains("-worker-"));
            assertThrows(IllegalStateException.class, () -> captured.get().highSequence());
            var bad = executor.readSnapshot(reader, IDENTITY, BUDGET, view -> view);
            var failure = assertThrows(CompletionException.class, () -> bad.result().toCompletableFuture().join());
            assertInstanceOf(IllegalArgumentException.class, failure.getCause());
        } finally { release.countDown(); }
    }

    @Test void notificationBackpressureRetainsAdmissionButReleasesEveryIoSlotAndSnapshot() throws Exception {
        var reader = new RecordingReader();
        var queryRelease = new CountDownLatch(1);
        var callbackEntered = new CountDownLatch(1);
        var callbackRelease = new CountDownLatch(1);
        var secondQueryClosed = new CountDownLatch(1);
        reader.afterClose = () -> { if (reader.closed.get() == 2) secondQueryClosed.countDown(); };
        try (var executor = executor(1, 2)) {
            var first = executor.readSnapshot(reader, IDENTITY, BUDGET, view -> { await(queryRelease); return 1; });
            first.result().thenRun(() -> { callbackEntered.countDown(); awaitUnchecked(callbackRelease); });
            queryRelease.countDown();
            await(callbackEntered);
            var second = executor.readSnapshot(reader, IDENTITY, BUDGET, view -> 2);
            await(secondQueryClosed);
            // 快照关闭先于工作线程交接；用短且有总上限的观察等待，不占满 CPU 或代跑通知。
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (executor.statistics().queuedNotifications() != 1 && System.nanoTime() < deadline) Thread.sleep(1);
            assertEquals(0, executor.statistics().executing());
            assertEquals(1, executor.statistics().queuedNotifications());
            assertEquals(2, executor.statistics().outstanding());
            assertThrows(ReadRejectedException.class, () -> executor.readSnapshot(reader, IDENTITY, BUDGET, view -> 3));
            var drained = executor.drain().toCompletableFuture();
            assertFalse(drained.isDone());
            assertFalse(executor.awaitDrain(Duration.ofMillis(20)));
            callbackRelease.countDown();
            assertEquals(2, second.result().toCompletableFuture().get(2, TimeUnit.SECONDS));
            drained.get(2, TimeUnit.SECONDS);
            assertEquals(0, executor.statistics().metadataBytes());
        } finally { queryRelease.countDown(); callbackRelease.countDown(); }
    }

    @Test void closeTimeoutDoesNotInterruptRunningReadOrPretendToDrain() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var interrupted = new AtomicBoolean();
        var executor = new BoundedReadExecutor(new ReadExecutionLimits(1, 1,
                BoundedReadExecutor.TASK_METADATA_BYTES, 1, 0, Duration.ofMillis(20)));
        try {
            var handle = executor.readSnapshot(new RecordingReader(), IDENTITY, BUDGET, view -> {
                entered.countDown();
                try { await(release); } catch (InterruptedException failure) { interrupted.set(true); throw failure; }
                return 1;
            });
            await(entered);
            assertThrows(IllegalStateException.class, executor::close);
            assertFalse(interrupted.get());
            assertEquals(1, executor.statistics().outstanding());
            release.countDown();
            assertEquals(1, handle.result().toCompletableFuture().get(2, TimeUnit.SECONDS));
            assertTrue(executor.awaitDrain(Duration.ofSeconds(2)));
        } finally { release.countDown(); executor.close(); }
    }

    private static BoundedReadExecutor executor(int workers, int maximum) {
        return new BoundedReadExecutor(new ReadExecutionLimits(workers, maximum,
                BoundedReadExecutor.TASK_METADATA_BYTES * maximum, 1, maximum - 1, Duration.ofSeconds(2)));
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        assertTrue(latch.await(2, TimeUnit.SECONDS), "Test coordination timed out");
    }

    private static void awaitUnchecked(CountDownLatch latch) {
        try { await(latch); } catch (InterruptedException error) { Thread.currentThread().interrupt(); throw new AssertionError(error); }
    }

    private static final class RecordingReader implements RealmStateReader {
        private final AtomicInteger opened = new AtomicInteger();
        private final AtomicInteger closed = new AtomicInteger();
        private final AtomicReference<String> lastThread = new AtomicReference<>();
        private Runnable afterClose = () -> { };

        @Override public Snapshot openSnapshot(RealmIdentity identity, ReadBudget budget) {
            opened.incrementAndGet();
            Thread owner = Thread.currentThread();
            lastThread.set(owner.getName());
            return new Snapshot() {
                private boolean finished;
                private void check() { assertSame(owner, Thread.currentThread()); assertFalse(finished); }
                @Override public RealmIdentity identity() { check(); return identity; }
                @Override public long highSequence() { check(); return 42; }
                @Override public Optional<VersionedState> find(ObjectKey key) { check(); return Optional.empty(); }
                @Override public Optional<VersionedState> findPlayerByAccount(long accountId) { check(); return Optional.empty(); }
                @Override public void scan(ObjectKind kind, Consumer<StateEntry> consumer) { check(); }
                @Override public void scanOwned(ObjectKind kind, String playerId, Consumer<StateEntry> consumer) { check(); }
                @Override public void scanActiveMarches(String playerId, Consumer<StateEntry> consumer) { check(); }
                @Override public void scanMovingMarches(Consumer<StateEntry> consumer) { check(); }
                @Override public void close() { check(); finished = true; closed.incrementAndGet(); afterClose.run(); }
            };
        }
    }
}
