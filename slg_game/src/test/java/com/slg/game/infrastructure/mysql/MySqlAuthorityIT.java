package com.slg.game.infrastructure.mysql;

import com.slg.core.battle.*;
import com.slg.core.economy.ResourceType;
import com.slg.game.application.commit.CommitResult;
import com.slg.game.application.port.*;
import com.slg.game.domain.persistence.*;
import com.slg.game.infrastructure.mysql.codec.PersistenceCodec;
import com.slg.game.infrastructure.mysql.repository.MySqlStateRepository;
import com.slg.game.infrastructure.mysql.session.MySqlSessions;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.*;

/** 用真实 MySQL 验证事务、MVCC 与故障分类；不连接开发库，不由模型单测代替锁语义。 */
@Timeout(60)
class MySqlAuthorityIT extends AuthoritySchemaFixture {
    private static final ReadBudget BUDGET = new ReadBudget(10_000, 16L * 1024 * 1024, Duration.ofSeconds(15));
    private static final String PLAYER = "1_7";
    private PersistenceCodec codec;
    private MySqlStateRepository repository;
    private MySqlRealmTransaction transaction;
    private MySqlRealmOwnership ownership;
    private MySqlRealmStateReader states;
    private MySqlRealmEventReader events;
    private WriterToken writer;

    @BeforeEach void wirePorts() throws Exception {
        codec = new PersistenceCodec(); repository = new MySqlStateRepository(codec);
        transaction = new MySqlRealmTransaction(sessions, repository, codec);
        ownership = new MySqlRealmOwnership(sessions, Duration.ofSeconds(30));
        writer = ownership.acquire(identity, UUID.randomUUID());
        states = new MySqlRealmStateReader(sessions, repository);
        events = new MySqlRealmEventReader(sessions, codec);
    }

    @Test void allStructuredRootsAndOrderedChildrenRoundTripInOneDecision() throws Exception {
        List<ObjectState> values = List.of(new PlayerState(PLAYER, 7, 1, 1, "玩家"),
                new ResourceState(PLAYER, ResourceType.WOOD, Long.MAX_VALUE),
                new TroopState(PLAYER, TroopType.INFANTRY, 120, 5, 2), new HeroState(PLAYER, "hero:z", "march"),
                march(Map.of(TroopType.INFANTRY, 120L, TroopType.LANCER, 40L), List.of("hero:z", "hero:a")),
                new GatherSiteState(150, 150, PLAYER, "march", 3),
                new TaskState("arrive", PLAYER, "march", TaskState.Type.MARCH_ARRIVE, TaskState.Status.PENDING, 3, 1000, null),
                battle("battle"));
        var decision = decision("all", values.stream().map(value -> new ObjectChange(value.key(), 0, value)).toList());
        assertEquals(1, committed(transaction.commit(writer, decision)).receipt().sequence());
        try (var snapshot = states.openSnapshot(identity, BUDGET)) {
            assertEquals(1, snapshot.highSequence());
            for (var value : values) assertEquals(new VersionedState(1, value), snapshot.find(value.key()).orElseThrow());
            assertEquals(values.getFirst(), snapshot.findPlayerByAccount(7).orElseThrow().state());
        }
        assertEquals(1, scalar("SELECT resource_type FROM game_player_resource"));
        assertEquals(1, scalar("SELECT status FROM game_march"));
        assertEquals(0, scalar("SELECT COUNT(*) FROM game_march WHERE last_seq=0"));
        assertEquals(decision, events.readAfter(identity, 0, 10, BUDGET).events().getFirst().decision());
        var work = new MySqlRealmWorkReader(sessions, repository);
        assertEquals(1, work.dueTasks(identity, 1000, null, 10, BUDGET).size());
        assertEquals(1, work.pendingBattles(identity, null, 10, BUDGET).size());
    }

    @Test void replayKeepsOriginalResultAndDifferentDigestCannotReplaceIt() throws Exception {
        var decision = new CommitDecision(new RequestKey("scope", "retry"), "digest", "NO_CHANGE", "{\"a\":1}",
                " { \"ok\" : true } ", List.of(), Map.of());
        var first = committed(transaction.commit(writer, decision));
        var repeated = committed(transaction.commit(writer, decision));
        assertTrue(first.applied()); assertFalse(repeated.applied()); assertEquals(first.receipt(), repeated.receipt());
        var changed = new CommitDecision(decision.request(), "other-digest", decision.factType(), "{}", "{}", List.of(), Map.of());
        assertEquals(CommitResult.Reason.REQUEST_CONFLICT, rejected(transaction.commit(writer, changed)));
        var observation = new MySqlRealmReceiptReader(sessions, codec).observe(identity, decision.request(), BUDGET);
        assertEquals(first.receipt(), ((RealmReceiptReader.Observation.Found) observation).receipt());
        assertEquals(1, scalar("SELECT high_seq FROM realm_commit_cursor"));
    }

    @Test void versionAndAbsenceDependenciesRejectWholeDecision() throws Exception {
        var wood = new ResourceState(PLAYER, ResourceType.WOOD, 100);
        committed(transaction.commit(writer, decision("seed", List.of(new ObjectChange(wood.key(), 0, wood)))));
        var bad = new CommitDecision(new RequestKey("scope", "bad"), "digest", "REJECT", "{}", "{}",
                List.of(new ObjectChange(wood.key(), 1, new ResourceState(PLAYER, ResourceType.WOOD, 50))), Map.of(wood.key(), 1L,
                new ObjectKey.Troop(PLAYER, TroopType.INFANTRY), 1L));
        assertEquals(CommitResult.Reason.VERSION_CONFLICT, rejected(transaction.commit(writer, bad)));
        var absent = new CommitDecision(new RequestKey("scope", "absent"), "digest", "REJECT", "{}", "{}", List.of(), Map.of(wood.key(), 0L));
        assertEquals(CommitResult.Reason.VERSION_CONFLICT, rejected(transaction.commit(writer, absent)));
        assertEquals(100, scalar("SELECT amount FROM game_player_resource"));
        assertEquals(1, scalar("SELECT high_seq FROM realm_commit_cursor"));
    }

    @Test void failureAfterObjectWritesRollsBackObjectsReceiptAndCursor() throws Exception {
        var wood = new ResourceState(PLAYER, ResourceType.WOOD, 100);
        committed(transaction.commit(writer, decision("seed", List.of(new ObjectChange(wood.key(), 0, wood)))));
        execute("CREATE TRIGGER reject_event BEFORE INSERT ON game_journal FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected'");
        var hero = new HeroState(PLAYER, "new-hero", null);
        assertThrows(StorageAccessException.class, () -> transaction.commit(writer, decision("fail", List.of(
                new ObjectChange(wood.key(), 1, new ResourceState(PLAYER, ResourceType.WOOD, 50)), new ObjectChange(hero.key(), 0, hero)))));
        assertEquals(100, scalar("SELECT amount FROM game_player_resource"));
        assertEquals(0, scalar("SELECT COUNT(*) FROM game_player_hero"));
        assertEquals(1, scalar("SELECT high_seq FROM realm_commit_cursor"));
        assertEquals(1, scalar("SELECT COUNT(*) FROM game_receipt"));
        execute("DROP TRIGGER reject_event");
        assertEquals(2, committed(transaction.commit(writer, decision("next", List.of()))).receipt().sequence());
    }

    @Test void concurrentIndependentDecisionsProduceContinuousCommittedPrefix() throws Exception {
        try (var executor = Executors.newFixedThreadPool(4)) {
            var jobs = new ArrayList<Future<RealmTransaction.CommitAttempt>>();
            for (int i = 0; i < 12; i++) {
                var decision = decision("parallel-" + i, List.of());
                jobs.add(executor.submit(() -> transaction.commit(writer, decision)));
            }
            var sequences = new TreeSet<Long>();
            for (var job : jobs) sequences.add(committed(job.get(20, TimeUnit.SECONDS)).receipt().sequence());
            assertEquals(12, sequences.size()); assertEquals(1L, sequences.first()); assertEquals(12L, sequences.last());
        }
        var page = events.readAfter(identity, 0, 20, BUDGET);
        assertEquals(12, page.high()); assertEquals(12, page.through()); assertEquals(12, page.events().size());
    }

    @Test void concurrentSameRequestCommitsOnceAndReturnsOriginalReceipt() throws Exception {
        var decision = decision("same-request", List.of());
        try (var executor = Executors.newFixedThreadPool(2)) {
            var start = new CountDownLatch(1);
            Callable<CommitResult.Committed> job = () -> { start.await(); return committed(transaction.commit(writer, decision)); };
            var left = executor.submit(job); var right = executor.submit(job); start.countDown();
            var a = left.get(15, TimeUnit.SECONDS); var b = right.get(15, TimeUnit.SECONDS);
            assertEquals(a.receipt(), b.receipt()); assertNotEquals(a.applied(), b.applied());
        }
        assertEquals(1, scalar("SELECT high_seq FROM realm_commit_cursor"));
        assertEquals(1, scalar("SELECT COUNT(*) FROM game_journal"));
    }

    @Test void snapshotKeepsWatermarkRootAndChildrenInOneView() throws Exception {
        var oldMarch = march(Map.of(TroopType.INFANTRY, 100L), List.of("z", "a"));
        committed(transaction.commit(writer, decision("seed", List.of(new ObjectChange(oldMarch.key(), 0, oldMarch)))));
        var snapshot = states.openSnapshot(identity, BUDGET);
        try {
            var changed = march(Map.of(TroopType.MARKSMAN, 80L), List.of("a"));
            committed(transaction.commit(writer, decision("change", List.of(new ObjectChange(changed.key(), 1, changed)))));
            assertEquals(1, snapshot.highSequence());
            assertEquals(oldMarch, snapshot.find(oldMarch.key()).orElseThrow().state());
            try (var fresh = states.openSnapshot(identity, BUDGET)) {
                assertEquals(2, fresh.highSequence()); assertEquals(changed, fresh.find(changed.key()).orElseThrow().state());
            }
            try (var executor = Executors.newSingleThreadExecutor()) {
                assertThrows(ExecutionException.class, () -> executor.submit(() -> snapshot.find(oldMarch.key())).get());
            }
        } finally { snapshot.close(); }
        assertThrows(Exception.class, () -> snapshot.find(oldMarch.key()));
    }

    @Test void childRowsCountTowardReadBudgetAndCompletedMarchesAreExcluded() throws Exception {
        var active = march(Map.of(TroopType.INFANTRY, 100L), List.of("z", "a"));
        var done = new MarchState("done", PLAYER, MarchState.Status.COMPLETED, MarchState.Mission.GATHER,
                0, 0, 150, 150, 100, 1000, 1, Map.of(), List.of(), null, "rules", Map.of());
        committed(transaction.commit(writer, decision("seed", List.of(new ObjectChange(active.key(), 0, active), new ObjectChange(done.key(), 0, done)))));
        try (var snapshot = states.openSnapshot(identity, new ReadBudget(3, 100_000, Duration.ofSeconds(5)))) {
            var failure = assertThrows(StorageAccessException.class, () -> snapshot.find(active.key()));
            assertEquals(StorageAccessException.Code.READ_BUDGET_EXCEEDED, failure.code());
        }
        try (var snapshot = states.openSnapshot(identity, BUDGET)) {
            var entries = new ArrayList<RealmStateReader.StateEntry>();
            snapshot.scanActiveMarches(PLAYER, entries::add);
            assertEquals(List.of(active), entries.stream().map(entry -> entry.state().state()).toList());
        }
    }

    @Test void eventGapsCorruptionExpiredAndFutureCursorsFailExplicitly() throws Exception {
        for (int i = 1; i <= 3; i++) committed(transaction.commit(writer, decision("event-" + i, List.of())));
        assertEquals(StorageAccessException.Code.INVALID_DATA,
                assertThrows(StorageAccessException.class, () -> events.readAfter(identity, 4, 10, BUDGET)).code());
        execute("UPDATE game_journal SET entry_hash=REPEAT(CHAR(0),32) WHERE journal_seq=2");
        assertEquals(StorageAccessException.Code.INVALID_DATA,
                assertThrows(StorageAccessException.class, () -> events.readAfter(identity, 0, 10, BUDGET)).code());
        execute("DELETE FROM game_journal WHERE journal_seq<=2");
        assertThrows(StorageAccessException.class, () -> events.readAfter(identity, 0, 10, BUDGET));
        execute("UPDATE realm_commit_cursor SET pruned_through=2");
        assertEquals(StorageAccessException.Code.CURSOR_EXPIRED,
                assertThrows(StorageAccessException.class, () -> events.readAfter(identity, 1, 10, BUDGET)).code());
        assertEquals(3, events.readAfter(identity, 2, 10, BUDGET).through());
    }

    @Test void commitAcknowledgementLossRequiresFenceAndResolvesToOriginalReceipt() throws Exception {
        var uncertainTransaction = transactionWithFault(Fault.AFTER_COMMIT);
        var decision = decision("lost-ack", List.of());
        assertInstanceOf(RealmTransaction.CommitAttempt.Uncertain.class, uncertainTransaction.commit(writer, decision));
        assertInstanceOf(RealmTransaction.CommitAttempt.Uncertain.class,
                transaction.resolveAfterFence(writer, writer, decision.request(), decision.digest()));
        var replacement = ownership.fence(writer);
        var resolved = committed(transaction.resolveAfterFence(writer, replacement, decision.request(), decision.digest()));
        assertFalse(resolved.applied()); assertEquals(1, resolved.receipt().sequence());
        assertEquals(CommitResult.Reason.OWNER_LOST, rejected(transaction.commit(writer, decision("stale", List.of()))));
    }

    @Test void failureBeforeServerCommitRemainsUnknownUntilFencedAbsenceIsProven() throws Exception {
        var decision = decision("before-commit", List.of());
        assertInstanceOf(RealmTransaction.CommitAttempt.Uncertain.class, transactionWithFault(Fault.BEFORE_COMMIT).commit(writer, decision));
        var replacement = ownership.fence(writer);
        assertEquals(CommitResult.Reason.ROLLED_BACK,
                rejected(transaction.resolveAfterFence(writer, replacement, decision.request(), decision.digest())));
        assertEquals(0, scalar("SELECT high_seq FROM realm_commit_cursor"));
    }

    @Test void confirmedCommitIsNotReversedByConnectionCloseFailure() throws Exception {
        assertTrue(committed(transactionWithFault(Fault.CLOSE).commit(writer, decision("close", List.of()))).applied());
        assertEquals(1, scalar("SELECT COUNT(*) FROM game_receipt"));
    }

    @Test void runtimeDmlAccountWorksWithoutSchemaPrivileges() throws Exception {
        String user = "slg_auth_" + UUID.randomUUID().toString().replace("-", "").substring(0, 20);
        String password = UUID.randomUUID().toString();
        String database;
        try (var connection = connection()) { database = connection.getCatalog(); }
        // 用户名/密码为本测试生成的 ASCII 值，schema 由隔离库工厂分配；不读取开发凭据。
        execute("CREATE USER '" + user + "'@'%' IDENTIFIED BY '" + password + "'");
        try {
            execute("GRANT SELECT,INSERT,UPDATE,DELETE ON `" + database + "`.* TO '" + user + "'@'%'");
            try (var runtimePool = new HikariDataSource()) {
                runtimePool.setJdbcUrl(url); runtimePool.setUsername(user); runtimePool.setPassword(password);
                runtimePool.setMaximumPoolSize(2); runtimePool.setMinimumIdle(0);
                var runtime = new MySqlRealmStorage(runtimePool, 5, Duration.ofSeconds(30));
                var state = new ResourceState(PLAYER, ResourceType.WOOD, 100);
                committed(runtime.transactions().commit(writer, decision("runtime-account", List.of(new ObjectChange(state.key(), 0, state)))));
                try (var snapshot = runtime.states().openSnapshot(identity, BUDGET)) {
                    assertEquals(state, snapshot.find(state.key()).orElseThrow().state());
                }
                try (var connection = runtimePool.getConnection(); var statement = connection.createStatement()) {
                    assertThrows(SQLException.class, () -> statement.execute("CREATE TABLE forbidden(id INT)"));
                }
            }
        } finally { execute("DROP USER '" + user + "'@'%'"); }
    }

    @Test void wrongDatasetUnsupportedSchemaAndCommittedPlaceholderFailClosed() throws Exception {
        assertEquals(StorageAccessException.Code.IDENTITY_MISMATCH, assertThrows(StorageAccessException.class,
                () -> states.openSnapshot(new RealmIdentity(1, UUID.randomUUID()), BUDGET)).code());
        execute("INSERT INTO game_player_resource VALUES (1,'1_7',1,100,1,0)");
        try (var snapshot = states.openSnapshot(identity, BUDGET)) {
            assertEquals(StorageAccessException.Code.INVALID_DATA, assertThrows(StorageAccessException.class,
                    () -> snapshot.find(new ObjectKey.Resource(PLAYER, ResourceType.WOOD))).code());
        }
        // 模拟更高版本部署留下的身份；数据库允许正版本，适配器只接受已实现的版本。
        execute("UPDATE realm_identity SET schema_version=2");
        assertEquals(StorageAccessException.Code.UNSUPPORTED_SCHEMA, assertThrows(StorageAccessException.class,
                () -> states.openSnapshot(identity, BUDGET)).code());
    }

    private MySqlRealmTransaction transactionWithFault(Fault fault) {
        var armed = new AtomicBoolean(true);
        DataSource injected = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{DataSource.class}, (proxy, method, args) -> {
            try {
                Object result = method.invoke(pool, args);
                if (!(result instanceof Connection connection)) return result;
                return Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{Connection.class}, (p, m, a) -> {
                    boolean target = m.getName().equals(fault == Fault.CLOSE ? "close" : "commit") && armed.compareAndSet(true, false);
                    if (target && fault == Fault.BEFORE_COMMIT) throw new SQLException("injected", "08006");
                    try {
                        Object value = m.invoke(connection, a);
                        if (target) throw new SQLException("injected", "08006");
                        return value;
                    } catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
            } catch (InvocationTargetException failure) { throw failure.getCause(); }
        });
        return new MySqlRealmTransaction(new MySqlSessions(injected, 5), repository, codec);
    }

    private BattleState battle(String id) {
        var input = new BattleInput(id, new Army(PLAYER, List.of(new TroopGroup("g", TroopType.INFANTRY, 1, 100))),
                new Army("npc", List.of()), 7, 10);
        var payload = codec.encodeBattleInput(input);
        String hash = HexFormat.of().formatHex(PersistenceCodec.hash(payload.json().getBytes(StandardCharsets.UTF_8)));
        return new BattleState(id, "march", PLAYER, BattleState.Status.PENDING, List.of("hero:z", "hero:a"), hash,
                ExpeditionBattleEngine.RULES_VERSION, 1, payload, null);
    }
    private static MarchState march(Map<TroopType, Long> troops, List<String> heroes) {
        return new MarchState("march", PLAYER, MarchState.Status.OUTBOUND, MarchState.Mission.GATHER,
                1.5, 2.5, 150, 150, 100, 1000, 1, troops, heroes, "arrive", "rules", Map.of(ResourceType.WOOD, 20L));
    }
    private static CommitDecision decision(String id, List<ObjectChange> changes) {
        return new CommitDecision(new RequestKey("scope", id), "digest-" + id, "FACT", "{}", "{\"ok\":true}", changes, Map.of());
    }
    private static CommitResult.Committed committed(RealmTransaction.CommitAttempt attempt) {
        return assertInstanceOf(CommitResult.Committed.class, assertInstanceOf(RealmTransaction.CommitAttempt.Settled.class, attempt).result());
    }
    private static CommitResult.Reason rejected(RealmTransaction.CommitAttempt attempt) {
        return assertInstanceOf(CommitResult.NotCommitted.class, assertInstanceOf(RealmTransaction.CommitAttempt.Settled.class, attempt).result()).reason();
    }
    private enum Fault { BEFORE_COMMIT, AFTER_COMMIT, CLOSE }
}
