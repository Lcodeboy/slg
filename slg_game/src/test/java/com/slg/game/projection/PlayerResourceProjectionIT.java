package com.slg.game.projection;

import com.slg.game.storage.RealmJournal.Change;
import com.slg.game.storage.RealmJournal.Entry;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.*;

/** 用真实 InnoDB 从已推进游标的 001 升级，观察原 GAME 写入方式、专用表以及消费进度。 */
@Timeout(60)
class PlayerResourceProjectionIT extends ProjectionSchemaFixture {
    private static final String MARCH = "c2c3028d07ed4b21aa22123e28578859";

    @Test void refusesLegacyUuidWithoutChangingItsArchivePlayerOrCursor() throws Exception {
        String id = UUID.randomUUID().toString();
        var old = new Change("player:" + id, 0, "{\"playerId\":\"" + id + "\",\"name\":\"Legacy\"}");
        store.apply(List.of(entry(1, id, "first", "PLAYER_CREATED", old)));
        // 即使没有 002 的表定义作为识别标记，也不能把 001 中的旧 UUID 强转成整数。
        assertEquals("23000", assertThrows(SQLException.class, () -> migrate(TYPED_MIGRATION)).getSQLState());
        migrate("002-player-resource-projections.sql");
        assertEquals("23000", assertThrows(SQLException.class, () -> migrate(TYPED_MIGRATION)).getSQLState());
        assertThrows(ProjectionConflict.class, store::requireRoleIdentitySchema);
        assertRow("SELECT account_id,player_id,object_version FROM projection_player", id, id, "1");
        assertEquals(1, store.checkpoint());
        assertEquals(1, number("SELECT COUNT(*) FROM projection_event"));
    }

    @Test void immutableFoundingRealmAndExactAccountSurviveProjectionInADifferentRealm() throws Exception {
        migrate(TYPED_MIGRATION);
        store.requireRoleIdentitySchema();
        // 模拟已由上层迁入的实体：当前卷为 1，角色仍携带建角区 17，账号取 signed long 的上界。
        // 这个用例只覆盖身份存储契约，不负责发起跨区迁移。
        String account = Long.toString(Long.MAX_VALUE), id = "17_" + account;
        String state = change("player:" + id, 0, 100).json()
                .replace("Commander " + id, "Commander").replace("\"currentRealmId\":17", "\"currentRealmId\":1");
        var role = new Change("player:" + id, 0, state);
        var binding = new Change("account-role:" + account, 0, "{\"playerId\":\"" + id + "\"}");
        var create = entry(1, account, "create", "PLAYER_CREATED", role, binding);
        assertEquals(1, store.apply(List.of(create)));
        assertEquals(1, store.apply(List.of(create)));
        assertRow("SELECT player_id,account_id,founding_realm_id,current_realm_id FROM projection_player",
                id, account, "17", "1");
        assertRow("SELECT object_id FROM projection_object WHERE object_type='account-role'", account);

        for (String invalid : List.of(state.replace("\"accountId\":\"" + account + "\"", "\"accountId\":" + account),
                state.replace("\"accountId\":\"" + account, "\"accountId\":\"0" + account),
                state.replace("\"foundingRealmId\":17", "\"foundingRealmId\":1"),
                state.replace("\"currentRealmId\":1", "\"currentRealmId\":17"))) {
            var update = new Change("player:" + id, 1, invalid);
            assertThrows(ProjectionConflict.class, () -> store.apply(List.of(entry(2, account, "bad", "PLAYER_UPDATED", update))));
            assertEquals(1, store.checkpoint());
        }
        String overflow = "9223372036854775808";
        var outsideBigint = new Change("player:17_" + overflow, 0, state.replace(account, overflow));
        assertThrows(ProjectionConflict.class, () -> store.apply(List.of(entry(2, overflow, "bad", "PLAYER_CREATED", outsideBigint))));
        assertEquals(1, store.checkpoint());
        assertRow("SELECT object_version,journal_seq FROM projection_player", "1", "1");
    }

    @Test void wrongEngineStopsMigrationBeforeAttachingTriggersToTheOldWriter() throws Exception {
        try (var conn = pool.getConnection(); var statement = conn.createStatement()) {
            statement.execute("CREATE TABLE projection_player (id INT PRIMARY KEY) ENGINE=MyISAM");
        }
        assertEquals("23000", assertThrows(SQLException.class, () -> migrate(TYPED_MIGRATION)).getSQLState());
        assertEquals(0, number("SELECT COUNT(*) FROM information_schema.triggers WHERE trigger_schema=DATABASE()"));
    }

    @Test void backfillRejectsAnOrphanAndACheckpointWithNoArchivedBoundary() throws Exception {
        store.apply(List.of(entry(1, "alice", "r1", "PLAYER_CREATED", change("player:1_101", 0, 100))));
        migrate(TYPED_MIGRATION);
        try (var conn = pool.getConnection(); var stmt = conn.createStatement()) {
            // 仅在本用例的临时库模拟错误备份：专用表出现来源不明的角色，或游标的末条归档丢失。
            stmt.executeUpdate("INSERT INTO projection_player (realm_id,player_id,account_id,founding_realm_id,current_realm_id,name,object_version,journal_seq)"
                    + " VALUES (1,'1_500',500,1,1,'Orphan',1,1)");
            assertEquals("23000", assertThrows(SQLException.class,
                    () -> stmt.execute("CALL projection_backfill_v3()")).getSQLState());
            assertEquals(2, number("SELECT COUNT(*) FROM projection_player"));
            stmt.executeUpdate("DELETE FROM projection_player WHERE player_id='1_500'");
            stmt.executeUpdate("UPDATE projection_checkpoint SET last_applied_seq=2 WHERE realm_id=1");
            assertEquals("23000", assertThrows(SQLException.class,
                    () -> stmt.execute("CALL projection_backfill_v3()")).getSQLState());
            assertEquals(2, number("SELECT last_applied_seq FROM projection_checkpoint WHERE realm_id=1"));
        }
    }

    @Test void existingCheckpointBackfillsAllRealmsAndRealQueriesFollowTheOriginalWriter() throws Exception {
        // U+00A0 是实际可创建的角色名（Java strip 会保留它），迁移应精确保留旧状态。
        String name = "\u00a0";
        String snapshot = change("player:1_101", 0, 100).json().replace("Commander 1_101", name);
        var player = entry(1, "alice", "create", "PLAYER_CREATED", new Change("player:1_101", 0, snapshot));
        var claim = entry(2, "timer:alice", "arrive", "MARCH_GATHER_STARTED", mine(0, true));
        assertEquals(2, store.apply(List.of(player, claim)));
        // 区服和坐标共同确定矿点；测试另一份相同账号、相同坐标的原卷，防止回填串区。
        var other = new MySqlProjectionStore(pool, 2, UUID.randomUUID());
        other.apply(List.of(entry(1, "alice", "create", "PLAYER_CREATED", change("player:2_101", 0, 100)),
                entry(2, "timer:alice", "other", "MARCH_GATHER_FINISHED", mine(0, false))));
        assertEquals(0, number("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE()"
                + " AND table_name IN ('projection_player','projection_resource_site')"));

        migrate(TYPED_MIGRATION);
        assertEquals(2, store.checkpoint());
        assertRow("SELECT player_id,account_id,founding_realm_id,current_realm_id,name,object_version,journal_seq FROM projection_player"
                + " WHERE realm_id=1 AND account_id=101", "1_101", "101", "1", "1", name, "1", "1");
        assertRow("SELECT x,y,occupant_march_id,generation,object_version,journal_seq FROM projection_resource_site"
                + " WHERE realm_id=1 AND occupant_player_id='1_101'", "150", "150", MARCH, "2", "1", "2");
        assertRow("SELECT occupant_player_id,occupant_march_id,generation FROM projection_resource_site"
                + " WHERE realm_id=2 AND x=150 AND y=150", null, null, "0");
        assertRow("SELECT GROUP_CONCAT(column_name ORDER BY seq_in_index) FROM information_schema.statistics"
                + " WHERE table_schema=DATABASE() AND table_name='projection_resource_site'"
                + " AND index_name='idx_resource_owner'", "realm_id,occupant_player_id,x,y");

        // 同页两次更新同一玩家，原投影会从 v1 合并到 v3。专用表必须接受完整版本跳跃。
        var finish = entry(3, "timer:alice", "finish", "MARCH_GATHER_FINISHED",
                change("player:1_101", 1, 110), mine(1, false));
        var returned = entry(4, "timer:alice", "return", "MARCH_COMPLETED", change("player:1_101", 2, 120));
        assertEquals(4, store.apply(List.of(claim, finish, returned)));
        assertEquals(4, store.apply(List.of(player, claim)));
        migrate(TYPED_MIGRATION); // 恢复重复部署时，仅用原对象最终快照补齐，不回放玩法、不改游标。
        assertRow("SELECT object_version,journal_seq FROM projection_player WHERE realm_id=1", "3", "4");
        assertRow("SELECT occupant_player_id,occupant_march_id,generation,object_version,journal_seq"
                + " FROM projection_resource_site WHERE realm_id=1 AND x=150 AND y=150", null, null, "0", "2", "3");
        assertEquals(4, store.checkpoint());
        assertEquals(2, other.checkpoint());
        assertEquals(4, number("SELECT COUNT(*) FROM projection_event WHERE realm_id=1"));
    }

    @Test void backfillAdvancesAcrossAFullPageAndRerunsWithoutRewritingHistory() throws Exception {
        var history = new ArrayList<Entry>();
        for (int index = 1; index <= 129; index++) {
            String id = "1_" + index;
            history.add(entry(index, id, "create", "PLAYER_CREATED", change("player:" + id, 0, index)));
        }
        assertEquals(129, store.apply(history));
        migrate(TYPED_MIGRATION);
        migrate(TYPED_MIGRATION);
        assertEquals(129, number("SELECT COUNT(*) FROM projection_player"));
        assertEquals(0, number("SELECT COUNT(*) FROM projection_object s LEFT JOIN projection_player p"
                + " ON p.realm_id=s.realm_id AND p.player_id=s.object_id WHERE p.player_id IS NULL"
                + " OR p.journal_seq<>s.journal_seq OR p.object_version<>s.object_version"));
        assertEquals(129, number("SELECT COUNT(*) FROM projection_event"));
        assertEquals(129, store.checkpoint());
    }

    @Test void oldWriterMayCreateThenUpdateAnObjectBetweenTheTwoTriggerInstallations() throws Exception {
        // 每个 CREATE TRIGGER 独立提交。若先装 INSERT，下面两笔旧事务将留下 typed v1、
        // generic v2，随后回填会误判成版本分歧。直接在正式迁移的语句边界安排这两笔写入。
        var wrote = new AtomicBoolean();
        migrate(TYPED_MIGRATION, sql -> {
            if (sql.startsWith("CREATE TRIGGER") && wrote.compareAndSet(false, true)) {
                assertEquals(1, store.apply(List.of(entry(1, "alice", "create", "PLAYER_CREATED",
                        change("player:1_101", 0, 100)))));
                assertEquals(2, store.apply(List.of(entry(2, "alice", "update", "PLAYER_UPDATED",
                        change("player:1_101", 1, 110)))));
            }
        });
        assertTrue(wrote.get());
        assertRow("SELECT player_id,object_version,journal_seq FROM projection_player WHERE realm_id=1",
                "1_101", "2", "2");
        assertEquals(2, store.checkpoint());
    }

    @Test void malformedLegacyObjectStopsThePageAndAValidRepairCanFinishTheMigration() throws Exception {
        var good = entry(1, "a", "create", "PLAYER_CREATED", change("player:1_101", 0, 100));
        var wrong = new Change("player:1_202", 0, change("player:1_303", 0, 100).json());
        assertEquals(2, store.apply(List.of(good, entry(2, "z", "create", "PLAYER_CREATED", wrong))));
        var failed = assertThrows(SQLException.class, () -> migrate(TYPED_MIGRATION));
        assertEquals("23000", failed.getSQLState());
        assertEquals(2, store.checkpoint());
        assertEquals(2, number("SELECT COUNT(*) FROM projection_object"));
        assertEquals(0, number("SELECT COUNT(*) FROM projection_player"), "A failed backfill page must roll back");

        // 补正应来源于原权威对象。测试通过后一条已提交版本修复 z 的实体身份，再重跑同一份迁移。
        assertEquals(3, store.apply(List.of(entry(3, "z", "repair", "PLAYER_REPAIRED", change("player:1_202", 1, 100)))));
        migrate(TYPED_MIGRATION);
        assertEquals(2, number("SELECT COUNT(*) FROM projection_player"));
        assertRow("SELECT player_id,object_version,journal_seq FROM projection_player WHERE realm_id=1"
                + " AND player_id='1_202'", "1_202", "2", "3");
        assertEquals(3, store.checkpoint());
    }

    @Test void conflictingTypedRowIsNeverSilentlyOverwrittenByBackfill() throws Exception {
        migrate(TYPED_MIGRATION);
        store.apply(List.of(entry(1, "a", "create", "PLAYER_CREATED", change("player:1_101", 0, 100))));
        try (var conn = pool.getConnection(); var stmt = conn.createStatement()) {
            stmt.executeUpdate("UPDATE projection_player SET name='different' WHERE realm_id=1 AND player_id='1_101'");
        }
        assertEquals("23000", assertThrows(SQLException.class, () -> migrate(TYPED_MIGRATION)).getSQLState());
        var next = entry(2, "a", "update", "PLAYER_UPDATED", change("player:1_101", 1, 90));
        assertThrows(ProjectionConflict.class, () -> store.apply(List.of(next)),
                "A later business update must not erase evidence of a same-version divergence");
        assertRow("SELECT name,object_version FROM projection_player WHERE realm_id=1", "different", "1");
        assertEquals(1, number("SELECT COUNT(*) FROM projection_event"));
        assertEquals(1, store.checkpoint());
    }

    @Test void onlineBackfillAndLiveWriterCompeteOnTheSameCheckpointWithoutRevertingTheNewState() throws Exception {
        store.apply(List.of(entry(1, "alice", "create", "PLAYER_CREATED", change("player:1_101", 0, 100)),
                entry(2, "timer:alice", "arrive", "MARCH_GATHER_STARTED", mine(0, true))));
        migrate(TYPED_MIGRATION);
        // 此库由用例专用。移除两条专用快照，模拟两个触发器已安装、对应回填页尚未轮到的时刻。
        try (var workers = Executors.newFixedThreadPool(2); var hold = pool.getConnection(); var stmt = hold.createStatement()) {
            stmt.executeUpdate("DELETE FROM projection_player WHERE realm_id=1");
            stmt.executeUpdate("DELETE FROM projection_resource_site WHERE realm_id=1");
            hold.setAutoCommit(false);
            try (var row = stmt.executeQuery("SELECT realm_id FROM projection_checkpoint WHERE realm_id=1 FOR UPDATE")) {
                assertTrue(row.next());
            }
            String renamed = change("player:1_101", 1, 120).json().replace("Commander 1_101", "Alicia");
            var update = entry(3, "timer:alice", "finish", "MARCH_GATHER_FINISHED",
                    new Change("player:1_101", 1, renamed), mine(1, false));
            var writer = workers.submit(() -> store.apply(List.of(update)));
            awaitCheckpointWaiters(hold, 1);
            var backfill = workers.submit(() -> {
                try (var conn = pool.getConnection(); var command = conn.createStatement()) {
                    command.execute("CALL projection_backfill_v3()");
                }
                return null;
            });
            // InnoDB 自身确认两条会话都等在本 schema 的 checkpoint 锁上，才让它们继续；
            // 任一方若绕过正常的锁顺序，这一断言就失败，而非依赖线程碰巧在同一时刻运行。
            try { awaitCheckpointWaiters(hold, 2); }
            finally { hold.rollback(); }
            assertEquals(3, writer.get(10, TimeUnit.SECONDS));
            backfill.get(10, TimeUnit.SECONDS);
        }
        assertRow("SELECT name,object_version,journal_seq FROM projection_player WHERE realm_id=1", "Alicia", "2", "3");
        assertRow("SELECT occupant_player_id,generation,object_version,journal_seq FROM projection_resource_site"
                + " WHERE realm_id=1 AND x=150 AND y=150", null, "0", "2", "3");
        assertEquals(3, number("SELECT COUNT(*) FROM projection_event"));
        assertEquals(3, store.checkpoint());
    }

    private static void awaitCheckpointWaiters(Connection holder, int expected) throws Exception {
        String sql = """
                SELECT COUNT(DISTINCT w.REQUESTING_THREAD_ID) FROM performance_schema.data_lock_waits w
                JOIN performance_schema.data_locks l ON w.ENGINE=l.ENGINE AND w.REQUESTING_ENGINE_LOCK_ID=l.ENGINE_LOCK_ID
                WHERE l.OBJECT_SCHEMA=DATABASE() AND l.OBJECT_NAME='projection_checkpoint'
                """;
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(4);
        try (var stmt = holder.createStatement()) {
            do {
                try (var rows = stmt.executeQuery(sql)) {
                    if (rows.next() && rows.getInt(1) >= expected) return;
                }
                Thread.sleep(10);
            } while (System.nanoTime() < end);
        }
        fail("Expected " + expected + " MySQL transactions to be waiting on this schema's realm checkpoint");
    }

    @Test void invalidMineCannotCommitItsArchivePlayerChangeOrCursor() throws Exception {
        migrate(TYPED_MIGRATION);
        store.apply(List.of(entry(1, "alice", "create", "PLAYER_CREATED", change("player:1_101", 0, 100))));
        // 坐标与 key 必须一致；释放时两个 owner 都要清空，代次必须是整数且与占用匹配。
        String valid = mine(0, true).json();
        for (String invalid : List.of(valid.replace("\"x\":150", "\"x\":650"),
                valid.replace("\"occupantPlayerId\":\"1_101\"", "\"occupantPlayerId\":null"),
                valid.replace("\"generation\":2", "\"generation\":\"2\""))) {
            var bad = entry(2, "timer:alice", "arrive", "MARCH_GATHER_STARTED",
                    change("player:1_101", 1, 90), new Change("resource:150-150", 0, invalid));
            assertThrows(ProjectionConflict.class, () -> store.apply(List.of(bad)));
            assertEquals(1, store.checkpoint());
            assertEquals(1, number("SELECT COUNT(*) FROM projection_event"));
            assertEquals(1, number("SELECT object_version FROM projection_player"));
            assertEquals(1, number("SELECT COUNT(*) FROM projection_object"));
            assertEquals(0, number("SELECT COUNT(*) FROM projection_resource_site"));
        }
    }

    private static Change mine(long expected, boolean occupied) {
        String snapshot = "{\"x\":150,\"y\":150," + (occupied
                ? "\"occupantPlayerId\":\"1_101\",\"occupantMarchId\":\"" + MARCH + "\",\"generation\":2}"
                : "\"occupantPlayerId\":null,\"occupantMarchId\":null,\"generation\":0}");
        return new Change("resource:150-150", expected, snapshot);
    }

    private void assertRow(String query, String... expected) throws SQLException {
        try (var conn = pool.getConnection(); var statement = conn.createStatement(); var rows = statement.executeQuery(query)) {
            assertTrue(rows.next(), "Expected one row for: " + query);
            for (int index = 0; index < expected.length; index++) assertEquals(expected[index], rows.getString(index + 1));
            assertFalse(rows.next(), "Expected a unique result for: " + query);
        }
    }
}
