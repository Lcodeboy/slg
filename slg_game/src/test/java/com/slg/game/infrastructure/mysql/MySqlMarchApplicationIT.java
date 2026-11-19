package com.slg.game.infrastructure.mysql;

import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.game.application.commit.*;
import com.slg.game.application.execution.OperationLimits;
import com.slg.game.application.march.*;
import com.slg.game.application.player.*;
import com.slg.game.application.port.*;
import com.slg.game.application.read.*;
import com.slg.game.domain.march.*;
import com.slg.game.domain.persistence.*;
import com.slg.game.infrastructure.mysql.codec.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.*;

/** 五个业务场景覆盖事务/资格交界，复用已有 UNKNOWN/缓存/执行器回归，不重复铺设框架测试。 */
@Timeout(45)
class MySqlMarchApplicationIT extends AuthoritySchemaFixture {
    private static final Duration WAIT = Duration.ofSeconds(5);

    @Test void concurrentStartsShareAccountLimitAndReplayNeverDeductsAgain() throws Exception {
        try (var runtime = new Runtime()) {
            create(runtime, 7);
            var calls = new ArrayList<PlayerCall<CommitResult>>();
            for (int i = 0; i < 4; i++) calls.add(runtime.marches.startMarch(actor(7), "start-" + i, order(1, List.of()), WAIT));
            int accepted = 0;
            for (var call : calls) {
                var outcome = runtime.players.outcome(committed(call).receipt());
                if (outcome.march() != null) accepted++;
                else assertEquals("MARCH_LIMIT", outcome.errorCode());
            }
            assertEquals(3, accepted);
            var player = result(runtime.players.getPlayer(actor(7), "player", WAIT));
            assertEquals(2700, player.troops().get(TroopType.INFANTRY).available());
            assertEquals(3, player.activeMarchIds().size()); assertEquals(1, player.version());
            var replay = committed(runtime.marches.startMarch(actor(7), "start-0", order(1, List.of()), WAIT));
            assertFalse(replay.applied());
            assertEquals(CommitResult.Reason.REQUEST_CONFLICT, assertInstanceOf(CommitResult.NotCommitted.class,
                    result(runtime.players.createPlayer(actor(7), "start-0", "new-name", WAIT))).reason());
            assertEquals(3, scalar("SELECT COUNT(*) FROM game_march"));
            assertEquals(3, scalar("SELECT COUNT(*) FROM game_due_task"));
            assertEquals(5, scalar("SELECT COUNT(*) FROM game_journal"));
        }
    }

    @Test void recallAtomicallyReplacesTaskWithoutReturningTroopsAndHidesForeignMarch() throws Exception {
        try (var runtime = new Runtime()) {
            create(runtime, 7); create(runtime, 8);
            var started = runtime.players.outcome(committed(runtime.marches.startMarch(actor(7), "start", order(1, null), WAIT)).receipt()).march();
            runtime.clock.now = 2000;
            var receipt = committed(runtime.marches.recallMarch(actor(7), "recall", started.marchId(), WAIT)).receipt();
            assertEquals(MarchState.Status.RETURNING, runtime.players.outcome(receipt).march().status());
            assertEquals(receipt, committed(runtime.marches.recallMarch(actor(7), "recall", started.marchId(), WAIT)).receipt());
            var player = result(runtime.players.getPlayer(actor(7), "player", WAIT));
            assertEquals(2900, player.troops().get(TroopType.INFANTRY).available());
            assertEquals(started.marchId(), player.heroes().getFirst().assignedMarchId());
            assertEquals(1, player.activeMarchIds().size());
            assertEquals(1, scalar("SELECT COUNT(*) FROM game_due_task WHERE status=3"));
            assertEquals(1, scalar("SELECT COUNT(*) FROM game_due_task WHERE status=1 AND generation=2"));
            assertEquals(0, scalar("SELECT COUNT(*) FROM game_gather_site"));
            var foreign = committed(runtime.marches.recallMarch(actor(8), "foreign", started.marchId(), WAIT));
            assertEquals("MARCH_NOT_FOUND", runtime.players.outcome(foreign.receipt()).errorCode());
            assertEquals(MarchState.Status.RETURNING, result(runtime.marches.getMarch(actor(7), "query", started.marchId(), WAIT)).status());
        }
    }

    @Test void overdueRecallCommitsGatherDeliveryAndOriginalRejectionExactlyOnce() throws Exception {
        try (var runtime = new Runtime()) {
            create(runtime, 7);
            var started = runtime.players.outcome(committed(runtime.marches.startMarch(actor(7), "start", order(1, null), WAIT)).receipt()).march();
            runtime.clock.now = 11_000; // 到达4000、采满7000、返城10000，统一在本次决定补齐。
            var caughtUp = committed(runtime.marches.recallMarch(actor(7), "late", started.marchId(), WAIT));
            assertEquals("INVALID_MARCH_STATE", runtime.players.outcome(caughtUp.receipt()).errorCode());
            assertEquals(caughtUp.receipt(), committed(runtime.marches.recallMarch(actor(7), "late", started.marchId(), WAIT)).receipt());
            var player = result(runtime.players.getPlayer(actor(7), "player", WAIT));
            assertEquals(3000, player.troops().get(TroopType.INFANTRY).available());
            assertEquals(500, player.resources().get(ResourceType.WOOD));
            assertNull(player.heroes().getFirst().assignedMarchId()); assertTrue(player.activeMarchIds().isEmpty());
            assertEquals(2, scalar("SELECT version FROM game_march"), "Folded stages change the root once");
            assertEquals(3, scalar("SELECT COUNT(*) FROM game_due_task WHERE status=2"));
            assertEquals(1, scalar("SELECT COUNT(*) FROM game_gather_site WHERE occupant_player_id IS NULL"));
            assertEquals(3, scalar("SELECT COUNT(*) FROM game_journal"));
            assertEquals(MarchState.Status.COMPLETED, result(runtime.marches.getMarch(actor(7), "history", started.marchId(), WAIT)).status());
        }
        // 重建应用实例仍从结构化行冷加载；旧拒绝回执不会让完成的采集再次发奖。
        try (var reloaded = new Runtime()) {
            assertEquals(500, result(reloaded.players.getPlayer(actor(7), "cold", WAIT)).resources().get(ResourceType.WOOD));
        }
    }

    @Test void npcArrivalDuringRecallPersistsFixedBattleInputButDoesNotDispatchWorker() throws Exception {
        try (var runtime = new Runtime()) {
            create(runtime, 7);
            var started = runtime.players.outcome(committed(runtime.marches.startMarch(actor(7), "start", order(2, null), WAIT)).receipt()).march();
            runtime.clock.now = 4000;
            var rejected = committed(runtime.marches.recallMarch(actor(7), "late", started.marchId(), WAIT));
            assertEquals("INVALID_MARCH_STATE", runtime.players.outcome(rejected.receipt()).errorCode());
            assertEquals(MarchState.Status.BATTLE_PENDING, result(runtime.marches.getMarch(actor(7), "query", started.marchId(), WAIT)).status());
            try (var snapshot = runtime.storage.states().openSnapshot(identity, runtime.budget)) {
                var battle = (BattleState) snapshot.find(new ObjectKey.Battle(started.marchId())).orElseThrow().state();
                assertEquals(BattleState.Status.PENDING, battle.status()); assertEquals(2, battle.marchVersion());
                assertTrue(battle.input().json().contains("\"type\":1"));
            }
            assertEquals(1, scalar("SELECT COUNT(*) FROM game_battle"));
            assertEquals(1, scalar("SELECT COUNT(*) FROM game_due_task WHERE status=2"));
        }
    }

    @Test void stalePlanningCannotExpandScopeOrWriteAReceiptAndOriginalIdCanRetry() throws Exception {
        try (var runtime = new Runtime()) {
            create(runtime, 7);
            var started = runtime.players.outcome(committed(runtime.marches.startMarch(actor(7), "start", order(1, null), WAIT)).receipt()).march();
            String id = started.marchId();
            var plan = result(runtime.players.executeQuery(actor(7), "stale", MarchModule.PLAN_RECALL,
                    runtime.codec.encodeInvocation(new MarchInvocation(actor(7), null, id, null)), Set.of(new ObjectKey.March(id)),
                    WAIT, value -> runtime.codec.decodePlan(value.bytes())));
            var incomplete = result(runtime.players.executeWrite(actor(7), "incomplete", MarchModule.RECALL,
                    runtime.codec.encodeInvocation(new MarchInvocation(actor(7), null, id, plan)), Set.of(new ObjectKey.March(id)), WAIT));
            assertEquals(CommitResult.Reason.INVALID_DECISION, assertInstanceOf(CommitResult.NotCommitted.class, incomplete).reason());
            assertEquals(2, scalar("SELECT COUNT(*) FROM game_journal"), "An incomplete scope must not reach SQL");
            runtime.clock.now = 2000;
            committed(runtime.marches.recallMarch(actor(7), "winner", id, WAIT));
            var keys = new HashSet<ObjectKey>(Set.of(new ObjectKey.March(id), new ObjectKey.Battle(id), new ObjectKey.GatherSite(150,150)));
            MarchIdentity.taskIds(id).forEach(task -> keys.add(new ObjectKey.Task(task)));
            var stale = result(runtime.players.executeWrite(actor(7), "stale", MarchModule.RECALL,
                    runtime.codec.encodeInvocation(new MarchInvocation(actor(7), null, id, plan)), keys, WAIT));
            assertEquals(CommitResult.Reason.VERSION_CONFLICT, assertInstanceOf(CommitResult.NotCommitted.class, stale).reason());
            assertEquals(3, scalar("SELECT COUNT(*) FROM game_journal"));
            assertInstanceOf(RealmReceiptReader.Observation.NotObserved.class,
                    result(runtime.players.getReceipt(actor(7), "receipt", "stale", WAIT)));
            assertEquals("INVALID_MARCH_STATE", runtime.players.outcome(committed(runtime.marches.recallMarch(actor(7), "stale", id, WAIT)).receipt()).errorCode());
        }
    }

    private static StartMarchOrder order(int mission, List<String> heroes) {
        int target = mission == 1 ? 150 : 120;
        return new StartMarchOrder(mission, target, target, 1, List.of(new StartMarchOrder.TroopSelection(1, 100)), heroes);
    }
    private static PlayerAccess actor(long account) { return new PlayerAccess(account, 1, Long.MAX_VALUE, new UUID(1,1).toString(), 1, new UUID(2,2).toString()); }
    private static <T> T result(PlayerCall<T> call) throws Exception { return call.result().toCompletableFuture().get(10, TimeUnit.SECONDS); }
    private static CommitResult.Committed committed(PlayerCall<CommitResult> call) throws Exception {
        return assertInstanceOf(CommitResult.Committed.class, result(call));
    }
    private static void create(Runtime runtime, long account) throws Exception { committed(runtime.players.createPlayer(actor(account), "create", "城主", WAIT)); }
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
        final MarchPayloadCodec codec = new MarchPayloadJsonCodec();
        final BoundedReadExecutor reads;
        final BoundedRealmCommitService commits;
        final PlayerApplication players;
        final MarchApplication marches;
        Runtime() throws Exception {
            var writer = storage.ownership().acquire(identity, UUID.randomUUID());
            var playerCodec = new PlayerPayloadJsonCodec();
            var sizer = new MySqlCommitPayloadSizer(new PersistenceCodec());
            reads = new BoundedReadExecutor(new ReadExecutionLimits(2, 32, 100_000, 2, 32, Duration.ofSeconds(10)));
            var directory = new PlayerDirectory(identity, PlayerSnapshotLoader.using(identity, storage.states(), reads, budget),
                    new PlayerCacheLimits(32, 64, 16_000_000, 1_000_000, Duration.ofMinutes(5)), playerCodec::measureSnapshot);
            commits = new BoundedRealmCommitService(writer, storage.transactions(), storage.ownership(), sizer,
                    new CommitQueueLimits(32, 16_000_000, 2, 100_000, 2, 2, WAIT, Duration.ofMillis(10), Duration.ofSeconds(5)));
            var module = new MarchModule(identity, storage.states(), reads, budget, codec, clock);
            players = new PlayerApplication(identity, directory, storage.receipts(), reads, budget, playerCodec, ignored -> true,
                    commits, sizer, new OperationLimits(32, 32_000_000, 64, 2, 2, 2, 2, 100_000, WAIT), Map.of(), List.of(module));
            marches = new MarchApplication(players, identity, codec);
        }
        @Override public void close() throws Exception {
            players.drain().toCompletableFuture().get(10, TimeUnit.SECONDS);
            commits.drain().toCompletableFuture().get(10, TimeUnit.SECONDS);
            reads.drain().toCompletableFuture().get(10, TimeUnit.SECONDS);
            storage.ownership().release(commits.snapshot().writer());
        }
    }
}
