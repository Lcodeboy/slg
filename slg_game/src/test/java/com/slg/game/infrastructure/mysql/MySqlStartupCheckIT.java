package com.slg.game.infrastructure.mysql;

import com.slg.game.application.commit.CommitResult;
import com.slg.game.application.port.RealmTransaction;
import com.slg.game.application.port.StorageAccessException;
import com.slg.game.domain.persistence.*;
import com.slg.game.infrastructure.mysql.runtime.AuthorityDatabaseSettings;
import com.slg.game.infrastructure.mysql.runtime.AuthorityDataSources;
import com.slg.game.infrastructure.mysql.runtime.MySqlStartupCheck;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.*;

/** 使用发布DDL和生产四池装配验证启动前只读检查；所有故障仅注入随机测试库。 */
@Timeout(60)
class MySqlStartupCheckIT extends AuthoritySchemaFixture {
    @Test void productionPoolsAndTableDmlAccountVerifyWithoutAcquiringOrRepairingOwnership() throws Exception {
        String user = "slg_start_" + UUID.randomUUID().toString().replace("-", "").substring(0, 18);
        String password = UUID.randomUUID().toString();
        String database;
        try (var connection = connection()) { database = connection.getCatalog(); }
        // 随机用户和schema属于本用例。按实际表用途授权，不授DDL、跨库或删除根/回执的权限。
        execute("CREATE USER '" + user + "'@'%' IDENTIFIED BY '" + password + "'");
        try {
            execute("GRANT SELECT ON `" + database + "`.* TO '" + user + "'@'%'");
            grant(database, user, "UPDATE", "realm_owner", "realm_owner_lease", "realm_commit_cursor");
            grant(database, user, "INSERT", "game_receipt", "game_journal");
            grant(database, user, "INSERT,UPDATE", "game_player", "game_player_resource", "game_player_troop",
                    "game_player_hero", "game_march", "game_gather_site", "game_due_task", "game_battle");
            grant(database, user, "INSERT,DELETE", "game_march_troop", "game_march_cargo", "game_march_hero", "game_battle_hero");
            String primaryUrl = url.split("\\?", 2)[0] + "?sslMode=DISABLED&allowPublicKeyRetrieval=true&connectionTimeZone=UTC";
            var settings = AuthorityDatabaseSettings.fromEnvironment(Map.of("SLG_GAME_JDBC_URL", primaryUrl,
                    "SLG_GAME_DB_USER", user, "SLG_GAME_DB_PASSWORD", password));
            try (var runtimePools = AuthorityDataSources.open(settings)) {
                var check = new MySqlStartupCheck(runtimePools.control(), 5);
                check.verify(identity);
                var empty = check.probe(identity);
                assertEquals(0, empty.highSequence()); assertEquals(0, empty.epoch()); assertFalse(empty.ownerActive());
                var storage = new MySqlRealmStorage(runtimePools.tx(), 5, Duration.ofSeconds(30));
                var writer = storage.ownership().acquire(identity, UUID.randomUUID());
                try {
                    committed(storage, writer, "first");
                    check.verify(identity);
                    var owned = check.probe(identity);
                    assertEquals(1, owned.highSequence()); assertEquals(writer.epoch(), owned.epoch());
                    assertEquals(writer.instanceId(), owned.ownerInstance());
                    assertTrue(owned.expiresAtMillis() > owned.databaseNowMillis());
                    try (var connection = runtimePools.tx().getConnection(); var statement = connection.createStatement()) {
                        assertThrows(SQLException.class, () -> statement.execute("CREATE TABLE forbidden_startup(id INT)"));
                        assertThrows(SQLException.class, () -> statement.executeUpdate("DELETE FROM game_journal"));
                    }
                    // map池独立且只读；其连接生命周期不会吞掉控制通道或把健康读取变成数据库写。
                    try (var map = runtimePools.map().getConnection()) { assertTrue(map.isReadOnly()); }
                    assertNotSame(runtimePools.map(), runtimePools.control());
                } finally { storage.ownership().release(writer); }
                check.verify(identity);
                var released = check.probe(identity);
                assertEquals(1, released.highSequence()); assertFalse(released.ownerActive());
                assertEquals(writer.epoch(), released.epoch());
            }
        } finally { execute("DROP USER '" + user + "'@'%'"); }
    }

    @Test void wrongIdentitySchemaMissingMovingIndexAndTailGapFailWithoutChangingOwner() throws Exception {
        var storage = new MySqlRealmStorage(pool, 5, Duration.ofSeconds(30));
        var writer = storage.ownership().acquire(identity, UUID.randomUUID());
        committed(storage, writer, "one"); committed(storage, writer, "two"); committed(storage, writer, "three");
        var check = new MySqlStartupCheck(pool, 5);
        check.verify(identity);
        var before = check.probe(identity);
        assertCode(StorageAccessException.Code.IDENTITY_MISMATCH,
                () -> check.verify(new RealmIdentity(identity.realmId(), UUID.randomUUID())));
        execute("UPDATE realm_identity SET schema_version=2 WHERE realm_id=1");
        assertCode(StorageAccessException.Code.UNSUPPORTED_SCHEMA, () -> check.verify(identity));
        execute("UPDATE realm_identity SET schema_version=1 WHERE realm_id=1");
        execute("DROP INDEX ix_march_moving ON game_march");
        assertCode(StorageAccessException.Code.UNSUPPORTED_SCHEMA, () -> check.verify(identity));
        execute("CREATE INDEX ix_march_moving ON game_march(realm_id,status,march_id)");
        check.verify(identity);
        // 管理员只为故障造数删中间事件；生产验库必须拒绝，不修改水位/owner或自动补事件。
        execute("DELETE FROM game_journal WHERE realm_id=1 AND journal_seq=2");
        assertCode(StorageAccessException.Code.INVALID_DATA, () -> check.verify(identity));
        var after = check.probe(identity);
        assertEquals(before.highSequence(), after.highSequence());
        assertEquals(before.epoch(), after.epoch());
        assertEquals(before.ownerInstance(), after.ownerInstance());
        assertEquals(before.expiresAtMillis(), after.expiresAtMillis());
        assertEquals(2, scalar("SELECT COUNT(*) FROM game_journal WHERE realm_id=1"));
    }

    private void grant(String database, String user, String privileges, String... tables) throws Exception {
        for (String table : tables) execute("GRANT " + privileges + " ON `" + database + "`." + table + " TO '" + user + "'@'%'");
    }
    private static void committed(MySqlRealmStorage storage, WriterToken writer, String request) throws StorageAccessException {
        var decision = new CommitDecision(new RequestKey("startup-it", request), request, "STARTUP_IT", "{}", "{}", List.of(), Map.of());
        var result = assertInstanceOf(RealmTransaction.CommitAttempt.Settled.class, storage.transactions().commit(writer, decision));
        assertInstanceOf(CommitResult.Committed.class, result.result());
    }
    private static void assertCode(StorageAccessException.Code expected, org.junit.jupiter.api.function.Executable action) {
        assertEquals(expected, assertThrows(StorageAccessException.class, action).code());
    }
}
