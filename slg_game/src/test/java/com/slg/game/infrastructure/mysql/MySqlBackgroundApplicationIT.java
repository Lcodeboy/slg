package com.slg.game.infrastructure.mysql;

import com.slg.core.battle.*;
import com.slg.core.economy.ResourceType;
import com.slg.game.application.background.*;
import com.slg.game.application.commit.*;
import com.slg.game.application.execution.OperationLimits;
import com.slg.game.application.march.*;
import com.slg.game.application.player.*;
import com.slg.game.application.port.*;
import com.slg.game.application.read.*;
import com.slg.game.domain.GameException;
import com.slg.game.domain.march.StartMarchOrder;
import com.slg.game.domain.persistence.*;
import com.slg.game.infrastructure.mysql.codec.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.*;

/** 真实 MySQL 验证离线任务、战果、恢复与 UNKNOWN；复用已有事务/执行器矩阵，仅保留四个完整场景。 */
@Timeout(60)
class MySqlBackgroundApplicationIT extends AuthoritySchemaFixture {
    private static final Duration WAIT = Duration.ofSeconds(5);

    @Test void offlineTasksSerializeMineCompetitionBlockCorruptionAndDeliverOnlyOnce() throws Exception {
        try (var runtime = new Runtime()) {
            for (long account : List.of(7L, 8L, 9L)) { create(runtime, account); start(runtime, account, 1, 100); }
            runtime.clock.now = 4000;
            execute("UPDATE game_due_task SET generation=99 WHERE player_id='1_9'");
            var tasks = runtime.due();
            var candidates = tasks.stream().filter(task -> !task.task().playerId().equals("1_9")).toList();
            runtime.sessionsValid.set(false); // 离线或会话被替换不阻止已经持久化的任务推进。
            var first = runtime.background.advance(candidates.get(0), WAIT);
            var second = runtime.background.advance(candidates.get(1), WAIT);
            var firstResult = committed(first); committed(second);
            assertFalse(committed(runtime.background.advance(candidates.get(0), WAIT)).applied());
            var bad = tasks.stream().filter(task -> task.task().playerId().equals("1_9")).findFirst().orElseThrow();
            var blocked = committed(runtime.background.advance(bad, WAIT));
            assertEquals("BLOCKED", runtime.codec.decodeOutcome(blocked.receipt().resultJson()).status());
            assertEquals(1, scalar("SELECT COUNT(*) FROM game_gather_site WHERE occupant_player_id IS NOT NULL"));
            assertEquals(1, scalar("SELECT COUNT(*) FROM game_due_task WHERE status=4"));
            assertEquals(1, scalar("SELECT version FROM game_march WHERE player_id='1_9'"));
            assertEquals(2900, scalar("SELECT available FROM game_player_troop WHERE player_id='1_9' AND troop_type=1"));
            runtime.clock.now = 10_000;
            for (int round = 0; round < 4; round++) for (var task : runtime.due()) committed(runtime.background.advance(task, WAIT));
            assertEquals(500, scalar("SELECT SUM(amount) FROM game_player_resource WHERE player_id IN ('1_7','1_8') AND resource_type=1"));
            assertEquals(6000, scalar("SELECT SUM(available) FROM game_player_troop WHERE player_id IN ('1_7','1_8') AND troop_type=1"));
            assertEquals(2, scalar("SELECT COUNT(*) FROM game_march WHERE status=5"));
            assertEquals(0, scalar("SELECT COUNT(*) FROM game_gather_site WHERE occupant_player_id IS NOT NULL"));
            long high = scalar("SELECT high_seq FROM realm_commit_cursor");
            assertEquals(firstResult.receipt(), committed(runtime.background.advance(candidates.get(0), WAIT)).receipt());
            assertEquals(high, scalar("SELECT high_seq FROM realm_commit_cursor"));
            runtime.sessionsValid.set(true);
            assertTrue(result(runtime.players.getPlayer(actor(7), "get", WAIT)).activeMarchIds().isEmpty());
        }
    }

    @Test void restartFindsPendingBattleRejectsBadResultAndSettlesCasualtiesAndCargoOnce() throws Exception {
        try (var initial = new Runtime()) {
            create(initial, 7); start(initial, 7, 2, 1000); initial.clock.now = 4000;
            committed(initial.background.advance(initial.due().getFirst(), WAIT));
            assertEquals(1, initial.pending().size()); // 尚未计算就关闭，权威 PENDING 足够恢复。
        }
        try (var runtime = new Runtime()) {
            runtime.clock.now = 6000;
            var candidate = runtime.pending().getFirst();
            var dispatch = result(runtime.background.prepareBattle(candidate, WAIT)).orElseThrow();
            var answer = new ExpeditionBattleEngine().simulate(dispatch.input());
            assertEquals(Outcome.ATTACKER_WIN, answer.outcome());
            var bad = new BattleResult(answer.battleId(), answer.outcome(), answer.rounds(), answer.attacker(), answer.defender(),
                    answer.rulesVersion(), answer.seed() + 1);
            long high = scalar("SELECT high_seq FROM realm_commit_cursor");
            assertEquals(CommitResult.Reason.INVALID_DECISION, assertInstanceOf(CommitResult.NotCommitted.class,
                    result(runtime.background.settle(candidate, bad, WAIT))).reason());
            assertEquals(high, scalar("SELECT high_seq FROM realm_commit_cursor"));
            assertEquals(1, scalar("SELECT COUNT(*) FROM game_battle WHERE status=1"));
            var settled = committed(runtime.background.settle(candidate, answer, WAIT));
            assertEquals(settled.receipt(), committed(runtime.background.settle(candidate, answer, WAIT)).receipt());
            assertEquals(2000, scalar("SELECT available FROM game_player_troop WHERE player_id='1_7' AND troop_type=1"));
            assertEquals(answer.attacker().groups().getFirst().wounded(), scalar("SELECT wounded FROM game_player_troop WHERE player_id='1_7' AND troop_type=1"));
            assertEquals(0, scalar("SELECT amount FROM game_player_resource WHERE player_id='1_7' AND resource_type=1"));
            assertEquals(BattleState.Status.SETTLED, result(runtime.background.getBattle(actor(7), "report", candidate.battle().battleId(), WAIT)).status());
            var denied = assertThrows(ExecutionException.class, () -> result(runtime.background.getBattle(actor(8), "private", candidate.battle().battleId(), WAIT)));
            assertEquals("BATTLE_NOT_FOUND", assertInstanceOf(GameException.class, rootCause(denied)).code());
            runtime.clock.now = 10_000;
            for (var task : runtime.due()) committed(runtime.background.advance(task, WAIT));
            var player = result(runtime.players.getPlayer(actor(7), "get", WAIT));
            assertEquals(2000 + answer.attacker().survivors(), player.troops().get(TroopType.INFANTRY).available());
            assertEquals(Map.of(ResourceType.WOOD, 500L, ResourceType.FOOD, 500L, ResourceType.GEMS, 10L), player.resources());
            assertNull(player.heroes().getFirst().assignedMarchId()); assertTrue(player.activeMarchIds().isEmpty());

            // 来源白名单只在进程内系统入口开放；客户端不能借注册名或系统scope占用保留预算。
            var row = new VersionedState(candidate.version(), candidate.battle());
            var input = runtime.codec.encodeInvocation(new BackgroundInvocation(7, row, null, answer));
            assertThrows(IllegalArgumentException.class, () -> runtime.players.executeWrite(actor(7), "fake", BackgroundModule.SETTLE, input, Set.of(), WAIT));
            assertThrows(IllegalArgumentException.class, () -> runtime.players.executeSystemWrite(7, runtime.codec.request(row), CommitRequest.Origin.CLIENT,
                    BackgroundModule.SETTLE, input, Set.of(), WAIT));
            assertThrows(IllegalArgumentException.class, () -> runtime.players.executeSystemWrite(7, new RequestKey("account:7", "fake"), CommitRequest.Origin.BATTLE_RESULT,
                    BackgroundModule.SETTLE, input, Set.of(), WAIT));
        }
    }

    @Test void pumpReleasesPlayerDuringWorkerAndDrainPreservesPendingForNextRuntime() throws Exception {
        var worker = new CompletableFuture<BattleResult>(); var entered = new CompletableFuture<BattleInput>();
        try (var runtime = new Runtime()) {
            create(runtime, 7); start(runtime, 7, 2, 1000); runtime.clock.now = 4000;
            runtime.ready.set(false);
            var pump = runtime.pump((input, hash, rules) -> { entered.complete(input); return worker; });
            pump.start();
            try {
                assertFalse(pump.pollOnce());
                runtime.ready.set(true);
                var input = entered.get(10, TimeUnit.SECONDS);
                assertEquals(2000, result(runtime.players.getPlayer(actor(7), "during-worker", WAIT)).troops().get(TroopType.INFANTRY).available());
                await(() -> runtime.directory.statistics().pins() == 0);
                var draining = pump.drain().toCompletableFuture();
                assertFalse(draining.isDone(), "Remote computation still holds a bounded background slot");
                worker.complete(new ExpeditionBattleEngine().simulate(input));
                draining.get(10, TimeUnit.SECONDS);
                assertTrue(pump.snapshot().closed()); assertEquals(0, pump.snapshot().retainedBytes());
                assertEquals(1, scalar("SELECT COUNT(*) FROM game_battle WHERE status=1"), "No new settlement begins after discovery stops");
            } finally {
                // 断言或超时失败也先停发现、解除测试 Worker 在途并真实排空，再关闭其依赖的执行器和数据源。
                pump.drain();
                worker.completeExceptionally(new IllegalStateException("Test cleanup"));
                pump.close();
            }
        }
        try (var restarted = new Runtime()) {
            restarted.clock.now = 10_000;
            try (var pump = restarted.pump((input, hash, rules) -> CompletableFuture.completedFuture(new ExpeditionBattleEngine().simulate(input)))) {
                pump.start();
                await(() -> scalar("SELECT COUNT(*) FROM game_battle WHERE status=2") == 1);
                restarted.clock.now = 20_000;
                await(() -> scalar("SELECT COUNT(*) FROM game_march WHERE status=5") == 1);
                assertEquals(500, scalar("SELECT amount FROM game_player_resource WHERE resource_type=1"));
            }
        }
    }

    @Test void uncertainSystemCommitKeepsQualificationUntilFenceAndOriginalReceiptResolve() throws Exception {
        var resolving = new CountDownLatch(1); var allow = new CountDownLatch(1); var injected = new AtomicBoolean();
        UnaryOperator<RealmTransaction> uncertain = actual -> new RealmTransaction() {
            @Override public CommitAttempt commit(WriterToken writer, CommitDecision decision) throws StorageAccessException {
                var outcome = actual.commit(writer, decision);
                return decision.factType().equals("MARCH_TASK_ADVANCED") && injected.compareAndSet(false, true)
                        ? new CommitAttempt.Uncertain() : outcome;
            }
            @Override public CommitAttempt resolveAfterFence(WriterToken old, WriterToken next, RequestKey request, String digest) throws StorageAccessException {
                resolving.countDown();
                try { if (!allow.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Test fence timeout"); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IllegalStateException(interrupted); }
                return actual.resolveAfterFence(old, next, request, digest);
            }
        };
        try (var runtime = new Runtime(uncertain)) {
            try {
                create(runtime, 7); start(runtime, 7, 1, 100); runtime.clock.now = 4000;
                var call = runtime.background.advance(runtime.due().getFirst(), WAIT);
                assertTrue(resolving.await(10, TimeUnit.SECONDS));
                assertTrue(call.result().toCompletableFuture().cancel(false));
                assertTrue(runtime.directory.statistics().pins() > 0);
                var draining = runtime.players.drain().toCompletableFuture(); assertFalse(draining.isDone());
                assertEquals(1, scalar("SELECT COUNT(*) FROM game_gather_site WHERE occupant_player_id IS NOT NULL"));
                allow.countDown(); assertFalse(committed(call).applied()); draining.get(10, TimeUnit.SECONDS);
                assertEquals(0, runtime.directory.statistics().pins());
                assertEquals(3, scalar("SELECT COUNT(*) FROM game_journal"));
            } finally { allow.countDown(); }
        }
    }

    private static void create(Runtime runtime, long account) throws Exception { committed(runtime.players.createPlayer(actor(account), "create", "城主", WAIT)); }
    private static void start(Runtime runtime, long account, int mission, long count) throws Exception {
        int target = mission == 1 ? 150 : 120;
        committed(runtime.marches.startMarch(actor(account), "start", new StartMarchOrder(mission, target, target, 1,
                List.of(new StartMarchOrder.TroopSelection(1, count)), null), WAIT));
    }
    private static PlayerAccess actor(long account) { return new PlayerAccess(account, 1, Long.MAX_VALUE, new UUID(1,1).toString(), 1, new UUID(2,2).toString()); }
    private static <T> T result(PlayerCall<T> call) throws Exception { return call.result().toCompletableFuture().get(10, TimeUnit.SECONDS); }
    private static CommitResult.Committed committed(PlayerCall<CommitResult> call) throws Exception { return assertInstanceOf(CommitResult.Committed.class, result(call)); }
    private static Throwable rootCause(Throwable error) { while (error.getCause() != null) error = error.getCause(); return error; }
    @FunctionalInterface private interface Condition { boolean value() throws Exception; }
    private static void await(Condition condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.value() && System.nanoTime() < deadline) Thread.sleep(10);
        assertTrue(condition.value(), "Timed out waiting for background lifecycle");
    }
    private static final class MutableClock extends Clock {
        volatile long now = 1000;
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return Instant.ofEpochMilli(now); }
        @Override public long millis() { return now; }
    }
    private final class Runtime implements AutoCloseable {
        final MySqlRealmStorage storage = new MySqlRealmStorage(pool, 5, Duration.ofSeconds(30));
        final ReadBudget budget = new ReadBudget(1000, 1_000_000, WAIT);
        final MutableClock clock = new MutableClock();
        final AtomicBoolean ready = new AtomicBoolean(true), sessionsValid = new AtomicBoolean(true);
        final BackgroundCodec codec = new BackgroundJsonCodec();
        final BoundedReadExecutor reads;
        final BoundedRealmCommitService commits;
        final PlayerDirectory directory;
        final PlayerApplication players;
        final MarchApplication marches;
        final BackgroundApplication background;
        Runtime() throws Exception { this(UnaryOperator.identity()); }
        Runtime(UnaryOperator<RealmTransaction> transactions) throws Exception {
            var writer = storage.ownership().acquire(identity, UUID.randomUUID());
            var playerCodec = new PlayerPayloadJsonCodec(); var marchCodec = new MarchPayloadJsonCodec();
            var sizer = new MySqlCommitPayloadSizer(new PersistenceCodec());
            reads = new BoundedReadExecutor(new ReadExecutionLimits(3, 32, 100_000, 2, 32, Duration.ofSeconds(10)));
            directory = new PlayerDirectory(identity, PlayerSnapshotLoader.using(identity, storage.states(), reads, budget),
                    new PlayerCacheLimits(32, 64, 16_000_000, 1_000_000, Duration.ofMinutes(5)), playerCodec::measureSnapshot);
            commits = new BoundedRealmCommitService(writer, transactions.apply(storage.transactions()), storage.ownership(), sizer,
                    new CommitQueueLimits(32, 32_000_000, 4, 8_000_000, 2, 2, WAIT, Duration.ofMillis(10), Duration.ofSeconds(5)));
            players = new PlayerApplication(identity, directory, storage.receipts(), reads, budget, playerCodec, ignored -> sessionsValid.get(),
                    commits, sizer, new OperationLimits(32, 64_000_000, 64, 2, 2, 2, 4, 16_000_000, WAIT), Map.of(),
                    List.of(new MarchModule(identity, storage.states(), reads, budget, marchCodec, clock),
                            new BackgroundModule(identity, storage.states(), reads, budget, codec, marchCodec, clock)));
            marches = new MarchApplication(players, identity, marchCodec);
            background = new BackgroundApplication(players, identity, codec, playerCodec);
        }
        List<RealmWorkReader.DueTask> due() throws Exception { return storage.work().dueTasks(identity, clock.millis(), null, 32, budget); }
        List<RealmWorkReader.PendingBattle> pending() throws Exception { return storage.work().pendingBattles(identity, null, 32, budget); }
        BackgroundPump pump(BattleComputation worker) {
            return new BackgroundPump(identity, storage.work(), reads, budget, background, worker,
                    () -> ready.get() && commits.currentWriter().isPresent(), clock,
                    new BackgroundPumpLimits(4, 2, 8, 32_000_000, 32, Duration.ofMillis(10), Duration.ofMillis(10),
                            Duration.ofMillis(100), WAIT, Duration.ofSeconds(10)));
        }
        @Override public void close() throws Exception {
            players.drain().toCompletableFuture().get(10, TimeUnit.SECONDS);
            commits.drain().toCompletableFuture().get(10, TimeUnit.SECONDS);
            reads.drain().toCompletableFuture().get(10, TimeUnit.SECONDS);
            storage.ownership().release(commits.snapshot().writer());
        }
    }
}
