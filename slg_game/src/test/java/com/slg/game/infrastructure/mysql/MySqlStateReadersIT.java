package com.slg.game.infrastructure.mysql;

import com.slg.core.battle.*;
import com.slg.core.economy.ResourceType;
import com.slg.game.application.commit.CommitResult;
import com.slg.game.application.port.*;
import com.slg.game.domain.persistence.*;
import com.slg.game.infrastructure.mysql.codec.PersistenceCodec;
import com.slg.game.infrastructure.mysql.mapper.StateMapper;
import com.slg.game.infrastructure.mysql.repository.MySqlStateRepository;
import com.slg.game.infrastructure.mysql.row.StateRows;
import org.apache.ibatis.scripting.defaults.DefaultParameterHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/** 使用随机独立 MySQL 库验证结构化映射、真实索引和读取预算，不操作开发/实服数据。 */
@Timeout(60)
class MySqlStateReadersIT extends AuthoritySchemaFixture {
    private static final String PLAYER = "1_7";
    private static final ReadBudget BUDGET = new ReadBudget(10_000, 8L * 1024 * 1024, Duration.ofSeconds(15));
    private PersistenceCodec codec;
    private MySqlRealmTransaction transaction;
    private MySqlRealmStateReader states;
    private MySqlRealmWorkReader work;
    private WriterToken writer;

    @BeforeEach void wire() throws Exception {
        codec = new PersistenceCodec();
        var repository = new MySqlStateRepository(codec);
        transaction = new MySqlRealmTransaction(sessions, repository, codec);
        states = new MySqlRealmStateReader(sessions, repository);
        work = new MySqlRealmWorkReader(sessions, repository);
        writer = new MySqlRealmOwnership(sessions, Duration.ofSeconds(30)).acquire(identity, UUID.randomUUID());
    }

    @Test void everyStoredEnumValueRoundTripsThroughTypedRowsAndIntegerColumns() throws Exception {
        var values = new ArrayList<ObjectState>();
        for (var type : ResourceType.values()) values.add(new ResourceState(PLAYER, type, type.code() * 10L));
        for (var type : TroopType.values()) values.add(new TroopState(PLAYER, type, type.code() * 100L, 2, 3));
        for (var status : MarchState.Status.values()) values.add(march("march-" + status.code(), PLAYER, status));
        for (var type : TaskState.Type.values()) for (var status : TaskState.Status.values())
            values.add(new TaskState("task-" + type.code() + "-" + status.code(), PLAYER, "march", type, status,
                    2, 100, status == TaskState.Status.BLOCKED ? "blocked-for-test" : null));
        values.add(battle("pending", BattleState.Status.PENDING));
        values.add(battle("settled", BattleState.Status.SETTLED));
        seed(values);
        try (var snapshot = states.openSnapshot(identity, BUDGET)) {
            for (var value : values) assertEquals(value, snapshot.find(value.key()).orElseThrow().state());
        }
        assertEquals(3, scalar("SELECT COUNT(DISTINCT resource_type) FROM game_player_resource"));
        assertEquals(3, scalar("SELECT COUNT(DISTINCT troop_type) FROM game_player_troop"));
        assertEquals(5, scalar("SELECT COUNT(DISTINCT status) FROM game_march"));
        assertEquals(2, scalar("SELECT COUNT(DISTINCT mission) FROM game_march"));
        assertEquals(3, scalar("SELECT COUNT(DISTINCT type) FROM game_due_task"));
        assertEquals(4, scalar("SELECT COUNT(DISTINCT status) FROM game_due_task"));
        assertEquals(2, scalar("SELECT COUNT(DISTINCT status) FROM game_battle"));
        assertThrows(java.sql.SQLException.class, () -> execute("UPDATE game_player_resource SET resource_type=99 WHERE resource_type=1"));
        assertThrows(java.sql.SQLException.class, () -> execute("UPDATE game_due_task SET status=NULL WHERE status=1"));
    }

    @Test void ownedScansExcludeOtherPlayersAndRetainBinaryAndNumericKeyOrder() throws Exception {
        var values = new ArrayList<ObjectState>();
        for (long account : new long[]{7, 8}) {
            String owner = "1_" + account;
            values.add(new PlayerState(owner, account, 1, 1, "玩家" + account));
            values.add(new ResourceState(owner, ResourceType.FOOD, 20));
            values.add(new ResourceState(owner, ResourceType.WOOD, 10));
            values.add(new TroopState(owner, TroopType.MARKSMAN, 10, 0, 0));
            values.add(new HeroState(owner, "z", null));
            values.add(new HeroState(owner, "A", null));
            values.add(march("march-" + account, owner, MarchState.Status.OUTBOUND));
            values.add(march("done-" + account, owner, MarchState.Status.COMPLETED));
            values.add(new GatherSiteState((int) account, 10, owner, "march-" + account, 1));
            values.add(new TaskState("task-" + account, owner, "march-" + account, TaskState.Type.MARCH_ARRIVE,
                    TaskState.Status.PENDING, 1, 100, null));
        }
        values.add(new GatherSiteState(0, 0, null, null, 0));
        seed(values);
        try (var snapshot = states.openSnapshot(identity, BUDGET)) {
            for (var kind : ObjectKind.values()) {
                var entries = new ArrayList<RealmStateReader.StateEntry>();
                snapshot.scanOwned(kind, PLAYER, entries::add);
                var keys = entries.stream().map(RealmStateReader.StateEntry::key).toList();
                assertEquals(keys.stream().sorted(ObjectKey.ORDER).toList(), keys);
                assertTrue(entries.stream().allMatch(entry -> owner(entry.state().state()).equals(PLAYER)));
                if (kind == ObjectKind.RESOURCE) assertEquals(List.of(new ObjectKey.Resource(PLAYER, ResourceType.WOOD),
                        new ObjectKey.Resource(PLAYER, ResourceType.FOOD)), keys);
                if (kind == ObjectKind.HERO) assertEquals(List.of(new ObjectKey.Hero(PLAYER, "A"), new ObjectKey.Hero(PLAYER, "z")), keys);
            }
            var active = new ArrayList<RealmStateReader.StateEntry>();
            snapshot.scanActiveMarches(PLAYER, active::add);
            assertEquals(List.of(new ObjectKey.March("march-7")), active.stream().map(RealmStateReader.StateEntry::key).toList());
        }
    }

    @Test void activeMarchPlanUsesStatusIndexInsteadOfCompletedHistoryIndex() throws Exception {
        seed(List.of(march("active", PLAYER, MarchState.Status.OUTBOUND), march("done", PLAYER, MarchState.Status.COMPLETED)));
        try (var session = sessions.openRead()) {
            var statement = session.getConfiguration().getMappedStatement(StateMapper.class.getName() + ".marches");
            var parameters = Map.of("q", StateRows.Query.scan(1, null, PLAYER, true, 128));
            var bound = statement.getBoundSql(parameters);
            try (var explain = session.getConnection().prepareStatement("EXPLAIN " + bound.getSql())) {
                new DefaultParameterHandler(statement, parameters, bound).setParameters(explain);
                try (var rows = explain.executeQuery()) {
                    assertTrue(rows.next());
                    assertEquals("ix_march_active", rows.getString("key"));
                }
            }
        }
    }

    @Test void duePagesUseExclusiveTimeAndBinaryIdCursorWithFixedUpperBound() throws Exception {
        seed(List.of(task("z", 100, TaskState.Status.PENDING), task("A", 100, TaskState.Status.PENDING),
                task("b", 90, TaskState.Status.PENDING), task("later", 101, TaskState.Status.PENDING),
                task("done", 80, TaskState.Status.DONE)));
        var first = work.dueTasks(identity, 100, null, 2, BUDGET);
        assertEquals(List.of("b", "A"), first.stream().map(item -> item.task().taskId()).toList());
        var second = work.dueTasks(identity, 100, first.getLast().cursor(), 2, BUDGET);
        assertEquals(List.of("z"), second.stream().map(item -> item.task().taskId()).toList());
        assertTrue(work.dueTasks(identity, 100, second.getLast().cursor(), 2, BUDGET).isEmpty());
        assertThrows(UnsupportedOperationException.class, first::clear);
    }

    @Test void dueByteBudgetReturnsResumableShortPageAndRejectsFirstOversizeItem() throws Exception {
        var firstTask = task("a", 100, TaskState.Status.PENDING);
        seed(List.of(firstTask, task("b", 100, TaskState.Status.PENDING)));
        long bytesForOne = 32 + 16 + codec.encodedStateBytes(firstTask);
        var one = new ReadBudget(10, bytesForOne, Duration.ofSeconds(5));
        var first = work.dueTasks(identity, 100, null, 10, one);
        assertEquals(List.of("a"), first.stream().map(item -> item.task().taskId()).toList());
        var second = work.dueTasks(identity, 100, first.getLast().cursor(), 10, one);
        assertEquals(List.of("b"), second.stream().map(item -> item.task().taskId()).toList());
        assertEquals(StorageAccessException.Code.READ_BUDGET_EXCEEDED,
                assertThrows(StorageAccessException.class, () -> work.dueTasks(identity, 100, null, 10,
                        new ReadBudget(10, bytesForOne - 1, Duration.ofSeconds(5)))).code());
    }

    @Test void battlePagesCountHeroChildrenAndResumeWithoutRepeatingAnId() throws Exception {
        seed(List.of(battle("a", BattleState.Status.PENDING), battle("b", BattleState.Status.PENDING),
                battle("c", BattleState.Status.SETTLED)));
        var one = new ReadBudget(4, 100_000, Duration.ofSeconds(5)); // identity + root + 两名英雄。
        var first = work.pendingBattles(identity, null, 10, one);
        assertEquals(List.of("a"), first.stream().map(item -> item.battle().battleId()).toList());
        var second = work.pendingBattles(identity, "a", 10, one);
        assertEquals(List.of("b"), second.stream().map(item -> item.battle().battleId()).toList());
        assertTrue(work.pendingBattles(identity, "b", 10, one).isEmpty());
        assertEquals(StorageAccessException.Code.READ_BUDGET_EXCEEDED,
                assertThrows(StorageAccessException.class, () -> work.pendingBattles(identity, null, 10,
                        new ReadBudget(3, 100_000, Duration.ofSeconds(5)))).code());
    }

    @Test void snapshotFailureInvalidatesFurtherReadsAndConsumerFailureIsNotReclassified() throws Exception {
        seed(List.of(new ResourceState(PLAYER, ResourceType.WOOD, 10)));
        try (var snapshot = states.openSnapshot(identity, new ReadBudget(2, 100_000, Duration.ofSeconds(5)))) {
            assertEquals(StorageAccessException.Code.READ_BUDGET_EXCEEDED,
                    assertThrows(StorageAccessException.class, () -> snapshot.find(new ObjectKey.Resource(PLAYER, ResourceType.WOOD))).code());
            assertThrows(IllegalStateException.class, () -> snapshot.find(new ObjectKey.Resource(PLAYER, ResourceType.WOOD)));
        }
        try (var snapshot = states.openSnapshot(identity, BUDGET)) {
            var callbackFailure = new IllegalArgumentException("consumer failure");
            assertSame(callbackFailure, assertThrows(IllegalArgumentException.class,
                    () -> snapshot.scan(ObjectKind.RESOURCE, ignored -> { throw callbackFailure; })));
            assertThrows(IllegalStateException.class, snapshot::highSequence);
        }
    }

    @Test void storedSourceBeyondSnapshotWatermarkAndZeroPlaceholderBothFailClosed() throws Exception {
        var state = new ResourceState(PLAYER, ResourceType.WOOD, 10);
        seed(List.of(state));
        execute("UPDATE game_player_resource SET last_seq=2");
        try (var snapshot = states.openSnapshot(identity, BUDGET)) {
            assertEquals(StorageAccessException.Code.INVALID_DATA,
                    assertThrows(StorageAccessException.class, () -> snapshot.find(state.key())).code());
        }
        execute("UPDATE game_player_resource SET last_seq=0");
        try (var snapshot = states.openSnapshot(identity, BUDGET)) {
            assertEquals(StorageAccessException.Code.INVALID_DATA,
                    assertThrows(StorageAccessException.class, () -> snapshot.find(state.key())).code());
        }
    }

    private void seed(List<? extends ObjectState> states) throws Exception {
        var changes = states.stream().map(state -> new ObjectChange(state.key(), 0, state)).toList();
        var decision = new CommitDecision(new RequestKey("reader", UUID.randomUUID().toString()), "digest", "SEED",
                "{}", "{}", changes, Map.of());
        var settled = assertInstanceOf(RealmTransaction.CommitAttempt.Settled.class, transaction.commit(writer, decision));
        assertInstanceOf(CommitResult.Committed.class, settled.result());
    }
    private static MarchState march(String id, String player, MarchState.Status status) {
        boolean timed = status == MarchState.Status.OUTBOUND || status == MarchState.Status.GATHERING || status == MarchState.Status.RETURNING;
        return new MarchState(id, player, status, status == MarchState.Status.BATTLE_PENDING ? MarchState.Mission.ATTACK_NPC : MarchState.Mission.GATHER,
                0, 0, 100, 100, 0, 1000, 1, Map.of(TroopType.INFANTRY, 10L), List.of("z", "a"),
                timed ? "task-" + id : null, "rules", Map.of(ResourceType.WOOD, 20L));
    }
    private static TaskState task(String id, long due, TaskState.Status status) {
        return new TaskState(id, PLAYER, "march", TaskState.Type.MARCH_ARRIVE, status, 1, due, null);
    }
    private BattleState battle(String id, BattleState.Status status) {
        var input = new BattleInput(id, new Army(PLAYER, List.of(new TroopGroup("g", TroopType.INFANTRY, 1, 100))),
                new Army("npc", List.of()), 7, 10);
        var payload = codec.encodeBattleInput(input);
        String hash = HexFormat.of().formatHex(PersistenceCodec.hash(payload.json().getBytes(StandardCharsets.UTF_8)));
        return new BattleState(id, "march", PLAYER, status, List.of("z", "a"), hash, ExpeditionBattleEngine.RULES_VERSION,
                1, payload, status == BattleState.Status.SETTLED ? codec.encodeBattleResult(new ExpeditionBattleEngine().simulate(input)) : null);
    }
    private static String owner(ObjectState value) {
        return switch (value) {
            case PlayerState state -> state.playerId();
            case ResourceState state -> state.playerId();
            case TroopState state -> state.playerId();
            case HeroState state -> state.playerId();
            case MarchState state -> state.playerId();
            case GatherSiteState state -> state.occupantPlayerId();
            case TaskState state -> state.playerId();
            case BattleState state -> state.playerId();
        };
    }
}
