package com.slg.game.projection;

import com.slg.game.storage.CleanupCrashScenario;
import com.slg.game.storage.IsolatedCleanupVolume;
import com.slg.game.storage.RealmJournal.Change;
import com.slg.game.storage.RealmJournal.Decision;
import com.slg.game.storage.RealmJournal.Entry;
import com.slg.game.storage.RealmVolume;
import com.slg.game.storage.RealmVolumePreview;
import com.slg.game.storage.JournalRetentionPreview;
import com.slg.core.protocol.Json;
import com.sun.net.httpserver.HttpServer;
import org.rocksdb.Checkpoint;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import com.slg.test.MySqlTestDatabase;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(60)
class MySqlProjectionStoreIT extends ProjectionSchemaFixture {
    @BeforeEach void installTypedQueries() throws Exception {
        migrate(TYPED_MIGRATION);
    }

    @Test @Timeout(210)
    void isolatedCleanupSurvivesForcedGameRestartsAndTheMarchReturnsExactlyOnce(@TempDir Path temp) throws Exception {
        CleanupCrashScenario.run(temp, pool, url, MySqlTestDatabase.user(), MySqlTestDatabase.password(), volume);
    }

    @Test void actualCleanupKeepsAllKeysWhenTheSqlAnchorNamesDifferentData(@TempDir Path temp) throws Exception {
        try (var testVolume = IsolatedCleanupVolume.create(temp.toRealPath(), 1, volume)) {
            var first = entry(1, "alice", "r1", "PLAYER_CREATED", change("player:1_101", 0, 100));
            var departure = entry(2, "alice", "r2", "MARCH_STARTED", change("player:1_101", 1, 80));
            var third = entry(3, "alice", "r3", "COMMAND_REJECTED");
            var journal = testVolume.journal();
            for (var event : List.of(first, departure, third)) journal.commit(decision(event));
            testVolume.checkpoint();
            // 库、卷、请求、序号都是对的，SQL 里同一决定的对象内容却与 J2 不同。
            // 仅凭 c=2 看似可以清 J1；实际执行必须重新核对 Jc 的指纹并保留全部现场。
            var other = entry(2, "alice", "r2", "MARCH_STARTED", change("player:1_101", 1, 81));
            assertEquals(2, store.apply(List.of(first, other)));
            var result = testVolume.clean(pool, 1);
            assertEquals("BLOCKED", result.state());
            assertEquals("SQL_BOUNDARY_CONFLICT", result.consumers().get("mysql").state());
            assertNull(result.lastEligibleSeq());
            assertEquals(0, journal.inspectPage(0, 16).prunedThrough());
            for (long seq = 1; seq <= 3; seq++) assertTrue(testVolume.retains(seq));
            assertEquals(2, store.checkpoint());
        }
    }

    @Test void retentionProbeCannotInitializeSqlOrTrustTheSameNumberOnDifferentHistory() throws Exception {
        var http = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var observed = new AtomicLong();
        http.createContext("/health", exchange -> {
            long position = observed.get();
            byte[] json = Json.write(Map.of("status", "UP", "realmId", 1, "volumeId", volume, "journalSeq", position,
                    "projection", Map.of("state", "CAUGHT_UP", "lastAppliedSeq", position))).getBytes(StandardCharsets.UTF_8);
            try (exchange) {
                exchange.sendResponseHeaders(200, json.length);
                exchange.getResponseBody().write(json);
            }
        });
        try {
            http.start();
            var env = Map.of("SLG_GAME_PORT", Integer.toString(http.getAddress().getPort()), "SLG_PROJECTION_JDBC_URL", url,
                    "SLG_PROJECTION_DB_USER", MySqlTestDatabase.user(), "SLG_PROJECTION_DB_PASSWORD", MySqlTestDatabase.password());
            var first = entry(1, "a", "r1", "PLAYER_CREATED", change("player:1_101", 0, 100));
            assertEquals("SQL_CHECKPOINT_MISSING", RetentionCursorProbe.observe(env, 1, volume, seq -> first, 1).state());
            assertEquals(0, number("SELECT COUNT(*) FROM projection_checkpoint"));
            store.apply(List.of(first));
            assertEquals("CURSOR_CHANGED", RetentionCursorProbe.observe(env, 1, volume, seq -> first, 1).state());
            observed.set(1);
            assertEquals("CAUGHT_UP", RetentionCursorProbe.observe(env, 1, volume, seq -> first, 1).state());
            var divergent = entry(1, "a", "r1", "PLAYER_CREATED", change("player:1_101", 0, 666));
            assertEquals("SQL_BOUNDARY_CONFLICT",
                    RetentionCursorProbe.observe(env, 1, volume, seq -> divergent, 1).state());
            assertEquals("ORIGINAL_BOUNDARY_MISSING",
                    RetentionCursorProbe.observe(env, 1, volume, seq -> null, 1).state());
            assertEquals(1, store.checkpoint());
            assertEquals(1, number("SELECT COUNT(*) FROM projection_event"));
            assertEquals(100, number("SELECT JSON_EXTRACT(state_json,'$.resources.WOOD') FROM projection_object"));
        } finally {
            http.stop(0);
        }
    }

    @Test void overlapChecksTheWholeDecisionAndOnlyWritesItsNewSuffix() throws Exception {
        assertEquals(0, store.checkpoint());
        Entry create = entry(1, "alice", "r1", "PLAYER_CREATED", change("player:1_101", 0, 100));
        Entry march = entry(2, "alice", "r2", "MARCH_STARTED",
                change("player:1_101", 1, 90), change("march:m", 0, 10), change("task:t", 0, 1));
        Entry reject = entry(3, "alice", "r3", "COMMAND_REJECTED");
        Entry settle = entry(4, "battle:alice", "m", "NPC_BATTLE_SETTLED",
                change("player:1_101", 2, 96), change("battle:m", 0, 6));

        assertEquals(2, store.apply(List.of(create, march)));
        assertEquals(4, store.apply(List.of(march, reject, settle)));
        assertEquals(4, store.apply(List.of(create, march, reject, settle)));
        assertEquals(4, store.apply(List.of(create)));
        assertEquals(4, store.checkpoint());
        assertEquals(4, number("SELECT COUNT(*) FROM projection_event"));
        assertEquals(4, number("SELECT COUNT(*) FROM projection_object"));
        assertEquals(1, number("SELECT COUNT(*) FROM projection_event WHERE fact_type='COMMAND_REJECTED' AND journal_seq=3"));
        assertEquals(3, number("SELECT object_version FROM projection_object WHERE object_type='player'"));
        assertEquals(96, number("SELECT JSON_EXTRACT(state_json,'$.resources.WOOD') FROM projection_object WHERE object_type='player'"));
        assertEquals(4, number("SELECT journal_seq FROM projection_object WHERE object_type='player'"));
        // 即使 ID、命令摘要及 fact_json 完全相同，恢复过程中替换了快照内容仍是另一份日志。
        Entry divergent = entry(1, "alice", "r1", "PLAYER_CREATED", change("player:1_101", 0, 666));
        assertThrows(ProjectionConflict.class, () -> store.apply(List.of(divergent)));
        assertEquals(4, store.checkpoint());
        // 同一业务键不能在另一个序号重复入账（区别于整批原样重投）。
        assertThrows(ProjectionConflict.class, () -> store.apply(List.of(entry(5, "alice", "r1", "PLAYER_CREATED"))));
        assertEquals(4, number("SELECT COUNT(*) FROM projection_event"));
    }

    @Test void liveVolumeDryRunCombinesRealSqlAndRestoredSnapshotWithoutAdvancingEither(@TempDir Path temporary)
            throws Exception {
        Path original = Files.createDirectory(temporary.toRealPath().resolve("realm"));
        Path snapshot = temporary.toRealPath().resolve("verified-checkpoint");
        RealmVolume.initialize(original, 1, volume);
        var first = entry(1, "alice", "r1", "PLAYER_CREATED", change("player:1_101", 0, 100));
        var second = entry(2, "alice", "r2", "MARCH_STARTED", change("player:1_101", 1, 80));
        var third = entry(3, "alice", "r3", "MARCH_COMPLETED", change("player:1_101", 2, 120));
        try (var initial = RealmVolume.open(original, 1, volume)) {
            initial.journal().commit(decision(first));
        }
        try (var options = new Options().setCreateIfMissing(false);
             var primary = RocksDB.open(options, original.toString()); var checkpoint = Checkpoint.create(primary)) {
            checkpoint.createCheckpoint(snapshot.toString());
        }
        try (var live = RealmVolume.open(original, 1, volume)) {
            var journal = live.journal();
            journal.commit(decision(second));
            journal.commit(decision(third));
            assertEquals(2, store.apply(List.of(first, second)));

            var http = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            http.createContext("/health", exchange -> {
                byte[] json = Json.write(Map.of("status", "UP", "realmId", 1, "volumeId", volume, "journalSeq", 3,
                        "projection", Map.of("state", "CATCHING_UP", "lastAppliedSeq", 2))).getBytes(StandardCharsets.UTF_8);
                try (exchange) {
                    exchange.sendResponseHeaders(200, json.length);
                    exchange.getResponseBody().write(json);
                }
            });
            try {
                http.start();
                var env = Map.of("SLG_GAME_PORT", Integer.toString(http.getAddress().getPort()), "SLG_PROJECTION_JDBC_URL", url,
                        "SLG_PROJECTION_DB_USER", MySqlTestDatabase.user(), "SLG_PROJECTION_DB_PASSWORD", MySqlTestDatabase.password(),
                        "SLG_REALM_JOURNAL_KEEP_ENTRIES", "1");
                var report = RealmVolumePreview.observe(original, 1, volume, snapshot, env);
                var preview = (JournalRetentionPreview.Result) report.get("journalRetention");
                assertTrue(preview.dryRun());
                assertEquals("CANDIDATE", preview.state(), report.toString());
                assertEquals(1L, preview.firstEligibleSeq());
                assertEquals(1L, preview.lastEligibleSeq());
                assertEquals(1L, preview.snapshot().sequence());
                assertTrue(preview.snapshot().restoreAndReplayVerified());
                assertEquals(List.of("consumer:mysql", "recoverySnapshot"), preview.limitingFactors());

                var missing = RealmVolumePreview.observe(original, 1, volume, temporary.resolve("absent"), env);
                var stopped = (JournalRetentionPreview.Result) missing.get("journalRetention");
                assertEquals("BLOCKED", stopped.state());
                assertNull(stopped.lastEligibleSeq());
                assertTrue(stopped.blockers().contains("VERIFIED_SNAPSHOT_MISSING"));
                assertNotNull(missing.get("snapshotDiagnostic"));
            } finally {
                http.stop(0);
            }
            // 连正向候选 J1 也保留在原卷。SQL 的覆盖范围仍是 [1,2]，预演不补写第 3 条。
            assertEquals(3, journal.highSequence());
            assertEquals(List.of(first, second, third), journal.entriesAfter(0, 3));
            assertEquals(2, store.checkpoint());
            assertEquals(2, number("SELECT COUNT(*) FROM projection_event"));
            assertEquals(80, number("SELECT JSON_EXTRACT(state_json,'$.resources.WOOD') FROM projection_object"));
        }
    }

    private static Decision decision(Entry entry) {
        return new Decision(entry.scope(), entry.requestId(), entry.digest(), entry.factType(),
                entry.factJson(), entry.resultJson(), entry.changes());
    }

    @Test void gapsAndBrokenObjectVersionsRollBackArchiveAndCursorTogether() throws Exception {
        var first = entry(1, "a", "r1", "PLAYER_CREATED", change("player:1_101", 0, 100));
        var second = entry(2, "a", "r2", "MARCH_STARTED", change("player:1_101", 1, 90));
        assertEquals(1, store.apply(List.of(first)));
        var gap = entry(3, "a", "r3", "MARCH_COMPLETED");
        assertThrows(ProjectionConflict.class, () -> store.apply(List.of(gap)));
        assertThrows(ProjectionConflict.class, () -> store.apply(List.of(first, gap)));

        // 第二条以不存在的行军版本为基线；错误发生时玩家更新和事实归档已经在同一事务内执行。
        var missing = entry(3, "a", "r3", "MARCH_COMPLETED", change("march:m", 1, 10));
        assertThrows(ProjectionConflict.class, () -> store.apply(List.of(second, missing)));
        assertEquals(1, store.checkpoint());
        assertEquals(1, number("SELECT COUNT(*) FROM projection_event"));
        assertEquals(1, number("SELECT object_version FROM projection_player"));
        assertEquals(100, number("SELECT JSON_EXTRACT(state_json,'$.resources.WOOD') FROM projection_object"));

        // 同一页折叠热点快照也不能遮盖漏掉的中间版本；未知类型不能被静默跳过。
        var broken = entry(3, "a", "r3", "MARCH_COMPLETED", change("player:1_101", 3, 80));
        var unknown = entry(3, "a", "r3", "MARCH_COMPLETED", change("unknown:x", 0, 10));
        assertThrows(ProjectionConflict.class, () -> store.apply(List.of(second, broken)));
        assertThrows(ProjectionConflict.class, () -> store.apply(List.of(second, unknown)));
        assertEquals(1, store.checkpoint());
        assertEquals(2, store.apply(List.of(second)));
    }

    @Test void jsonNumberOutsideMysqlRangeIsAPermanentDataError() throws Exception {
        assertEquals(0, store.checkpoint());
        var first = entry(1, "a", "r1", "PLAYER_CREATED", change("player:1_101", 0, 100));
        // 此数字是合法的 JSON 文本，但超出 MySQL 二进制 JSON 所能存储的数值范围。
        // 归档和游标均应回滚，诊断只含区服和序号范围，不把业务内容或原始驱动报错写入日志。
        var malformedPlayer = change("player:1_101", 1, 100).json().replace("\"WOOD\":100", "\"WOOD\":1e10000");
        var bad = entry(2, "a", "r2", "MARCH_STARTED", new Change("player:1_101", 1, malformedPlayer));
        var error = assertThrows(ProjectionConflict.class, () -> store.apply(List.of(first, bad)));
        assertTrue(error.getMessage().contains("batch 1..2"));
        assertFalse(error.getMessage().contains("WOOD"));
        assertEquals(0, store.checkpoint());
        assertEquals(0, number("SELECT COUNT(*) FROM projection_event"));
        assertEquals(0, number("SELECT COUNT(*) FROM projection_object"));
    }

    @Test void restoringOnlyTheCheckpointTableCannotSkipAnUnarchivedTail() throws Exception {
        var create = entry(1, "a", "r1", "PLAYER_CREATED", change("player:1_101", 0, 100));
        store.apply(List.of(create));
        try (var conn = pool.getConnection(); var stmt = conn.createStatement()) {
            // 模拟运维把更晚的游标表单独恢复，业务归档仍只有第 1 条。
            stmt.executeUpdate("UPDATE projection_checkpoint SET last_applied_seq=2 WHERE realm_id=1");
        }
        assertThrows(ProjectionConflict.class, store::checkpoint);
        assertThrows(ProjectionConflict.class, () -> store.apply(List.of(entry(3, "a", "r3", "MARCH_COMPLETED"))));
        assertEquals(1, number("SELECT COUNT(*) FROM projection_event"));
    }

    @Test void restartingConsumerStopsBeforeAppendingToADivergentArchive(@TempDir Path realmDir) throws Exception {
        RealmVolume.initialize(realmDir, 1, volume);
        try (var realm = RealmVolume.open(realmDir, 1, volume)) {
            var journal = realm.journal();
            Entry origin = entry(1, "alice", "r1", "PLAYER_CREATED", change("player:1_101", 0, 100));
            Entry next = entry(2, "alice", "r2", "MARCH_STARTED",
                    change("player:1_101", 1, 90), change("march:m", 0, 10));
            for (Entry event : List.of(origin, next)) {
                journal.commit(new Decision(event.scope(), event.requestId(), event.digest(), event.factType(),
                        event.factJson(), event.resultJson(), event.changes()));
            }
            // MySQL 声称已消费第 1 条，卷身份、流水字段与序号均匹配，但对应的是另一份对象状态。
            // 若新进程只信 last_applied_seq 并从第 2 条开始写，该差异会被新状态覆盖而无法发现。
            store.apply(List.of(entry(1, "alice", "r1", "PLAYER_CREATED", change("player:1_101", 0, 666))));
            var settings = Map.of("SLG_PROJECTION_JDBC_URL", url, "SLG_PROJECTION_DB_USER", MySqlTestDatabase.user(),
                    "SLG_PROJECTION_DB_PASSWORD", MySqlTestDatabase.password());
            try (var consumer = RealmProjection.configure(journal, 1, volume, settings)) {
                consumer.start();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                Map<String, Object> metrics;
                do {
                    metrics = consumer.metrics(journal.highSequence());
                    if ("BLOCKED".equals(metrics.get("state"))) break;
                    Thread.sleep(10);
                } while (System.nanoTime() < deadline);
                assertEquals("BLOCKED", metrics.get("state"), metrics.toString());
                assertTrue(metrics.get("diagnostic").toString().contains("Conflicting replay at sequence 1"));
            }
            assertEquals(1, store.checkpoint());
            assertEquals(1, number("SELECT COUNT(*) FROM projection_event"));
            assertEquals(1, number("SELECT COUNT(*) FROM projection_object"));
            assertEquals(666, number("SELECT JSON_EXTRACT(state_json,'$.resources.WOOD') FROM projection_object"));
            assertEquals(2, journal.highSequence());
            assertEquals(next, journal.entry(2));
            assertEquals(change("player:1_101", 1, 90).json(), journal.read("player:1_101").json());
        }
    }

    @Test void connectionLostOnEitherSideOfCommitRecoversUsingOnlyTheSqlCursor() throws Exception {
        var first = entry(1, "a", "r1", "PLAYER_CREATED", change("player:1_101", 0, 100));
        var page = List.of(first);
        assertEquals(0, store.checkpoint());
        // 两个故障点都已执行完更新游标的 SQL，应用层收到相同的连接异常，却无法判断 MySQL 是否已提交。
        var before = new MySqlProjectionStore(breakCommit(false), 1, volume);
        assertThrows(SQLTransientConnectionException.class, () -> before.apply(page));
        assertEquals(0, store.checkpoint());
        assertEquals(0, number("SELECT COUNT(*) FROM projection_object"));
        assertEquals(0, number("SELECT COUNT(*) FROM projection_player"));
        assertEquals(0, number("SELECT COUNT(*) FROM projection_event"));

        var after = new MySqlProjectionStore(breakCommit(true), 1, volume);
        assertThrows(SQLTransientConnectionException.class, () -> after.apply(page));
        var restarted = new MySqlProjectionStore(pool, 1, volume);
        assertEquals(1, restarted.checkpoint());
        assertEquals(1, restarted.apply(page));
        assertEquals(1, number("SELECT COUNT(*) FROM projection_event"));
        assertEquals(1, number("SELECT object_version FROM projection_object"));
        assertEquals(1, number("SELECT object_version FROM projection_player"));
    }

    @Test void concurrentConsumersSerializeAndSeparateRealmIdentitiesStayIsolated() throws Exception {
        var page = List.of(entry(1, "a", "r1", "PLAYER_CREATED", change("player:1_101", 0, 100)));
        var start = new CountDownLatch(1);
        var other = new MySqlProjectionStore(pool, 1, volume);
        try (var workers = Executors.newFixedThreadPool(2)) {
            var left = workers.submit(() -> { start.await(); return store.apply(page); });
            var right = workers.submit(() -> { start.await(); return other.apply(page); });
            start.countDown();
            assertEquals(1, left.get(10, TimeUnit.SECONDS));
            assertEquals(1, right.get(10, TimeUnit.SECONDS));
        }
        assertEquals(1, number("SELECT COUNT(*) FROM projection_event"));
        assertEquals(1, number("SELECT object_version FROM projection_object"));
        var wrongVolume = new MySqlProjectionStore(pool, 1, UUID.randomUUID());
        assertThrows(ProjectionConflict.class, wrongVolume::checkpoint);
        assertThrows(ProjectionConflict.class, () -> wrongVolume.apply(page));
        assertThrows(ProjectionConflict.class, () -> new MySqlProjectionStore(pool, 2, volume).checkpoint());
        var secondRealm = new MySqlProjectionStore(pool, 2, UUID.randomUUID());
        assertEquals(0, secondRealm.checkpoint());
        assertEquals(1, secondRealm.apply(List.of(entry(1, "a", "r1", "PLAYER_CREATED", change("player:2_101", 0, 100)))));
        assertEquals(2, number("SELECT COUNT(*) FROM projection_object"));
    }

    /** 只替换 JDBC commit 的返回边界。SQL、行锁、回滚与实际持久化均由同一个真实 MySQL 执行。 */
    private DataSource breakCommit(boolean serverCommitted) {
        return (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, args) -> {
                    Object value = invoke(method, pool, args);
                    if (!(value instanceof Connection connection)) return value;
                    return Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class},
                            (wrapped, operation, arguments) -> {
                                if (!operation.getName().equals("commit")) return invoke(operation, connection, arguments);
                                if (serverCommitted) connection.commit();
                                throw new SQLTransientConnectionException("Simulated loss of commit reply", "08006");
                            });
                });
    }

    private static Object invoke(Method method, Object target, Object[] args) throws Throwable {
        try { return method.invoke(target, args); }
        catch (InvocationTargetException call) { throw call.getCause(); }
    }
}
