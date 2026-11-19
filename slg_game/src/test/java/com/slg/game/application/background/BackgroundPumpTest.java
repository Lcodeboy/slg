package com.slg.game.application.background;

import com.slg.core.battle.*;
import com.slg.game.application.commit.CommitResult;
import com.slg.game.application.player.PlayerCall;
import com.slg.game.application.port.*;
import com.slg.game.application.read.*;
import com.slg.game.domain.persistence.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.*;

/** 聚焦扫描/用例交界；事务幂等、任务代次及结算守恒由真实数据库流程统一验证。 */
@Timeout(15)
class BackgroundPumpTest {
    private static final RealmIdentity REALM = new RealmIdentity(1, UUID.randomUUID());

    @Test void shortPagesKeepFixedCutoffAndSkipBusyAccountWithoutBlockingAnotherOwner() throws Exception {
        var first = due("a", 7, 10); var busy = due("b", 7, 20); var independent = due("c", 8, 30);
        var all = List.of(first, busy, independent);
        var bounds = new CopyOnWriteArrayList<Long>();
        var reader = new Reader() {
            @Override public List<DueTask> dueTasks(RealmIdentity realm, long through, DueCursor after, int limit, ReadBudget budget) {
                assertTrue(Thread.currentThread().getName().contains("cold-read"));
                bounds.add(through);
                // 模拟字节预算只够一条，即使limit更大，非空短页也不代表扫描结束。
                return all.stream().filter(task -> after == null || task.task().taskId().compareTo(after.taskId()) > 0).limit(1).toList();
            }
        };
        try (var fixture = new Fixture(reader)) {
            poll(fixture.pump); await(() -> fixture.work.tasks.size() == 1 && fixture.pump.snapshot().scans() == 0);
            fixture.clock.now = 100;
            poll(fixture.pump); await(() -> bounds.size() == 2 && fixture.pump.snapshot().scans() == 0);
            assertEquals(1, fixture.work.tasks.size(), "The second task of a busy account remains pending");
            poll(fixture.pump); await(() -> fixture.work.tasks.size() == 2 && fixture.pump.snapshot().scans() == 0);
            assertTrue(fixture.work.tasks.containsKey("c"));
            poll(fixture.pump); await(() -> bounds.size() == 4 && fixture.pump.snapshot().scans() == 0);
            fixture.work.completeTasks();
            await(() -> fixture.pump.snapshot().taskInFlight() == 0);
            poll(fixture.pump); await(() -> bounds.size() == 5 && fixture.pump.snapshot().scans() == 0);
            assertEquals(List.of(50L, 50L, 50L, 50L, 100L), bounds);
            assertTrue(fixture.pump.snapshot().retainedBytes() <= fixture.limits.maxRetainedBytes());
        }
    }

    @Test void failedScanBacksOffAndReadinessStopsDiscoveryUntilRecovery() throws Exception {
        var attempts = new AtomicInteger();
        var reader = new Reader() {
            @Override public List<DueTask> dueTasks(RealmIdentity realm, long through, DueCursor after, int limit, ReadBudget budget)
                    throws StorageAccessException {
                if (attempts.incrementAndGet() == 1) throw new StorageAccessException(StorageAccessException.Code.UNAVAILABLE, "Injected outage");
                return after == null ? List.of(due("a", 7, 10)) : List.of();
            }
        };
        try (var fixture = new Fixture(reader)) {
            poll(fixture.pump); await(() -> fixture.pump.snapshot().failures() == 1 && fixture.pump.snapshot().scans() == 0);
            assertFalse(fixture.pump.healthy()); assertTrue(fixture.work.tasks.isEmpty());
            fixture.ready.set(false);
            assertFalse(fixture.pump.pollOnce()); assertEquals(1, attempts.get());
            fixture.ready.set(true);
            poll(fixture.pump); await(() -> fixture.work.tasks.size() == 1 && fixture.pump.snapshot().scans() == 0);
            assertTrue(fixture.pump.healthy()); assertEquals(2, attempts.get());
        }
    }

    @Test void drainWaitsForActualWorkerCompletionAndLeavesUnsubmittedResultPending() throws Exception {
        var candidate = pending();
        var reader = new Reader() {
            @Override public List<PendingBattle> pendingBattles(RealmIdentity realm, String after, int limit, ReadBudget budget) {
                return after == null ? List.of(candidate) : List.of();
            }
        };
        try (var fixture = new Fixture(reader)) {
            poll(fixture.pump); await(() -> fixture.computations.get() == 1 && fixture.pump.snapshot().scans() == 0);
            assertEquals(1, fixture.pump.snapshot().battleInFlight());
            assertEquals(0, fixture.work.settlements.get());
            var drain = fixture.pump.drain().toCompletableFuture();
            assertFalse(drain.isDone()); assertFalse(fixture.pump.pollOnce());
            assertTrue(fixture.pump.snapshot().retainedBytes() >= BackgroundPumpLimits.BATTLE_SLOT_BYTES);
            fixture.worker.complete(new ExpeditionBattleEngine().simulate(input()));
            drain.get(3, TimeUnit.SECONDS);
            assertEquals(0, fixture.work.settlements.get(), "Closing may discard pure computation, never a started commit");
            assertEquals(BattleState.Status.PENDING, candidate.battle().status());
            assertTrue(fixture.pump.snapshot().closed()); assertEquals(0, fixture.pump.snapshot().retainedBytes());
        }
    }

    private static RealmWorkReader.DueTask due(String id, long account, long due) {
        return new RealmWorkReader.DueTask(1, new TaskState(id, "1_" + account, "march-" + id,
                TaskState.Type.MARCH_ARRIVE, TaskState.Status.PENDING, 1, due, null));
    }
    private static RealmWorkReader.PendingBattle pending() {
        return new RealmWorkReader.PendingBattle(1, new BattleState("battle", "battle", "1_7", BattleState.Status.PENDING,
                List.of(), "input-hash", ExpeditionBattleEngine.RULES_VERSION, 2, new JsonPayload(1, "{}"), null));
    }
    private static BattleInput input() {
        return new BattleInput("battle", new Army("1_7", List.of(new TroopGroup("infantry", TroopType.INFANTRY, 1, 100))),
                new Army("npc", List.of(new TroopGroup("infantry", TroopType.INFANTRY, 1, 10))), 1, 100);
    }
    private static <T> PlayerCall<T> call(CompletableFuture<T> result) {
        return new PlayerCall<>() {
            @Override public CompletionStage<T> result() { return result.minimalCompletionStage(); }
            @Override public boolean cancelBeforeStart() { return false; }
        };
    }
    private static void await(BooleanSupplier condition) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() - until < 0) Thread.sleep(5);
        assertTrue(condition.getAsBoolean(), "Timed out awaiting background state");
    }
    private static void poll(BackgroundPump pump) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() - until < 0) {
            if (pump.pollOnce()) return;
            Thread.sleep(5);
        }
        fail("Timed out triggering bounded background scan");
    }

    private static class Reader implements RealmWorkReader {
        @Override public List<DueTask> dueTasks(RealmIdentity realm, long through, DueCursor after, int limit, ReadBudget budget)
                throws StorageAccessException { return List.of(); }
        @Override public List<PendingBattle> pendingBattles(RealmIdentity realm, String after, int limit, ReadBudget budget)
                throws StorageAccessException { return List.of(); }
    }
    private static final class Work implements BackgroundWork {
        final Map<String, CompletableFuture<CommitResult>> tasks = new ConcurrentHashMap<>();
        final AtomicInteger settlements = new AtomicInteger();
        @Override public PlayerCall<CommitResult> advance(RealmWorkReader.DueTask candidate, Duration wait) {
            var result = new CompletableFuture<CommitResult>(); tasks.put(candidate.task().taskId(), result); return call(result);
        }
        @Override public PlayerCall<Optional<BattleDispatch>> prepareBattle(RealmWorkReader.PendingBattle candidate, Duration wait) {
            return call(CompletableFuture.completedFuture(Optional.of(new BattleDispatch(candidate, input()))));
        }
        @Override public PlayerCall<CommitResult> settle(RealmWorkReader.PendingBattle candidate, BattleResult result, Duration wait) {
            settlements.incrementAndGet();
            return call(CompletableFuture.completedFuture(new CommitResult.NotCommitted(CommitResult.Reason.ROLLED_BACK)));
        }
        void completeTasks() { tasks.values().forEach(value -> value.complete(new CommitResult.NotCommitted(CommitResult.Reason.ROLLED_BACK))); }
    }
    private static final class MutableClock extends Clock {
        volatile long now = 50;
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return Instant.ofEpochMilli(now); }
        @Override public long millis() { return now; }
    }
    private static final class Fixture implements AutoCloseable {
        final Work work = new Work(); final MutableClock clock = new MutableClock(); final AtomicBoolean ready = new AtomicBoolean(true);
        final AtomicInteger computations = new AtomicInteger(); final CompletableFuture<BattleResult> worker = new CompletableFuture<>();
        final BoundedReadExecutor reads = new BoundedReadExecutor(new ReadExecutionLimits(2, 16, 100_000, 1, 16, Duration.ofSeconds(5)));
        final BackgroundPumpLimits limits = new BackgroundPumpLimits(4, 2, 4, 32L * 1024 * 1024, 16,
                Duration.ofDays(1), Duration.ofMillis(20), Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofSeconds(5));
        final BackgroundPump pump;
        Fixture(RealmWorkReader reader) {
            pump = new BackgroundPump(REALM, reader, reads, new ReadBudget(100, 4096, Duration.ofSeconds(2)), work,
                    (input, hash, rules) -> { computations.incrementAndGet(); return worker; }, ready::get, clock, limits);
        }
        @Override public void close() {
            pump.drain(); // 先关闭发现，再完成测试持有的阶段，避免清理期间又准入一项永不完成的任务。
            work.completeTasks(); worker.completeExceptionally(new IllegalStateException("Test completed"));
            try { pump.close(); } finally { reads.close(); }
        }
    }
}
