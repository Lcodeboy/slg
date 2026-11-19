package com.slg.game.infrastructure.mysql;

import com.slg.game.application.port.StorageAccessException;
import com.slg.game.domain.persistence.RealmIdentity;
import com.slg.game.domain.persistence.WriterToken;
import com.slg.game.infrastructure.mysql.mapper.RealmMetadataMapper;
import com.slg.game.infrastructure.mysql.session.MySqlSessions;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** 真 MySQL 验证锁等待、数据库时间及提交确认丢失；不能以 JVM 模拟锁代替隔离证据。 */
final class MySqlRealmOwnershipIT extends AuthoritySchemaFixture {
    private MySqlRealmOwnership ownership() { return new MySqlRealmOwnership(sessions, Duration.ofSeconds(30)); }

    @Test void liveOwnerCannotBeAcquiredAgainAndReleasedEpochIsNeverReused() throws Exception {
        var ownership = ownership();
        UUID instance = UUID.randomUUID();
        WriterToken first = ownership.acquire(identity, instance);
        assertEquals(1, first.epoch());
        assertEquals(StorageAccessException.Code.OWNERSHIP_CONFLICT,
                assertThrows(StorageAccessException.class, () -> ownership.acquire(identity, instance)).code());
        assertTrue(ownership.renew(first));
        ownership.release(first);
        ownership.release(first);
        WriterToken next = ownership.acquire(identity, UUID.randomUUID());
        assertEquals(2, next.epoch());
        assertFalse(ownership.renew(first));
        ownership.release(first);
        assertTrue(ownership.renew(next));
        assertEquals(StorageAccessException.Code.OWNERSHIP_CONFLICT,
                assertThrows(StorageAccessException.class, () -> ownership.fence(first)).code());
    }

    @Test void expiredTokenCannotRenewAndOnlyNewEpochCanTakeOver() throws Exception {
        var ownership = ownership();
        WriterToken old = ownership.acquire(identity, UUID.randomUUID());
        expireLease();
        assertFalse(ownership.renew(old));
        WriterToken next = ownership.acquire(identity, UUID.randomUUID());
        assertEquals(old.epoch() + 1, next.epoch());
        assertFalse(ownership.renew(old));
        assertEquals(StorageAccessException.Code.OWNERSHIP_CONFLICT,
                assertThrows(StorageAccessException.class, () -> ownership.fence(old)).code());
    }

    @Test void fenceWaitsForOldBusinessOwnerLockButRenewDoesNot() throws Exception {
        var ownership = ownership();
        WriterToken old = ownership.acquire(identity, UUID.randomUUID());
        try (var business = sessions.openWrite(); var executor = Executors.newSingleThreadExecutor()) {
            var mapper = business.getMapper(RealmMetadataMapper.class);
            assertEquals(old.epoch(), mapper.lockOwnerShared(identity.realmId()).writerEpoch());
            // 普通 lease 读取不持行锁，因此 owner 共享锁下的长业务不会挡住心跳。
            mapper.lease(identity.realmId());
            assertTrue(executor.submit(() -> ownership.renew(old)).get(2, TimeUnit.SECONDS));
            var started = new CountDownLatch(1);
            var replacement = executor.submit(() -> { started.countDown(); return ownership.fence(old); });
            assertTrue(started.await(1, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> replacement.get(200, TimeUnit.MILLISECONDS));
            business.commit(true);
            WriterToken next = replacement.get(2, TimeUnit.SECONDS);
            assertEquals(old.epoch() + 1, next.epoch());
            assertTrue(ownership.renew(next));
            assertFalse(ownership.renew(old));
        }
    }

    @Test void wrongDatasetAndEpochOverflowFailWithoutReplacingOwner() throws Exception {
        var ownership = ownership();
        assertEquals(StorageAccessException.Code.IDENTITY_MISMATCH, assertThrows(StorageAccessException.class,
                () -> ownership.acquire(new RealmIdentity(1, UUID.randomUUID()), UUID.randomUUID())).code());
        assertEquals(0, scalar("SELECT writer_epoch FROM realm_owner WHERE realm_id=1"));
        WriterToken first = ownership.acquire(identity, UUID.randomUUID());
        execute("UPDATE realm_owner SET writer_epoch=? WHERE realm_id=1", Long.MAX_VALUE);
        execute("UPDATE realm_owner_lease SET writer_epoch=? WHERE realm_id=1", Long.MAX_VALUE);
        var exhausted = new WriterToken(identity, first.instanceId(), Long.MAX_VALUE);
        assertEquals(StorageAccessException.Code.OWNERSHIP_CONFLICT,
                assertThrows(StorageAccessException.class, () -> ownership.fence(exhausted)).code());
        expireLease();
        assertEquals(StorageAccessException.Code.OWNERSHIP_CONFLICT,
                assertThrows(StorageAccessException.class, () -> ownership.acquire(identity, UUID.randomUUID())).code());
        assertEquals(Long.MAX_VALUE, scalar("SELECT writer_epoch FROM realm_owner WHERE realm_id=1"));
    }

    @Test void fenceCommitAcknowledgementLossDoesNotReturnGuessedToken() throws Exception {
        var ownership = ownership();
        WriterToken old = ownership.acquire(identity, UUID.randomUUID());
        var faulty = new MySqlRealmOwnership(new MySqlSessions(failAfterCommit(), 5), Duration.ofSeconds(30));
        StorageAccessException error = assertThrows(StorageAccessException.class, () -> faulty.fence(old));
        assertEquals(StorageAccessException.Code.UNAVAILABLE, error.code());
        assertNull(error.getCause(), "Driver diagnostics must not leak credentials or raw SQL");
        assertEquals(old.epoch() + 1, scalar("SELECT writer_epoch FROM realm_owner WHERE realm_id=1"));
        assertEquals(StorageAccessException.Code.OWNERSHIP_CONFLICT,
                assertThrows(StorageAccessException.class, () -> ownership.fence(old)).code());
        assertFalse(ownership.renew(old));
        // 模拟停续期后租约到期，重新 acquire 才能获得已知 token，不猜测丢失的 epoch。
        expireLease();
        assertEquals(old.epoch() + 2, ownership.acquire(identity, old.instanceId()).epoch());
    }

    @Test void schemaRejectsUnknownCodesInvalidCursorAndPartialOccupancy() throws Exception {
        assertThrows(SQLException.class, () -> execute("UPDATE realm_commit_cursor SET pruned_through=1 WHERE realm_id=1"));
        assertThrows(SQLException.class, () -> execute("INSERT INTO game_player_resource VALUES (1,'1_1',99,10,1,1)"));
        assertThrows(SQLException.class, () -> execute("INSERT INTO game_player_troop VALUES (1,'1_1',1,-1,0,0,1,1)"));
        assertThrows(SQLException.class, () -> execute("INSERT INTO game_gather_site VALUES (1,0,0,'1_1',NULL,1,1,1)"));
        // 创建占位允许 0 来源序号，但负数仍由 DDL 拒绝；0 的已提交读取由 Repository 拒绝。
        assertEquals(1, execute("INSERT INTO game_player_resource VALUES (1,'1_1',1,0,1,0)"));
        assertThrows(SQLException.class, () -> execute("UPDATE game_player_resource SET last_seq=-1"));
        assertEquals(18, scalar("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE()"));
    }

    private void expireLease() throws SQLException {
        execute("UPDATE realm_owner_lease SET expires_at_millis="
                + "CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3))*1000 AS SIGNED)-1 WHERE realm_id=1");
    }

    private DataSource failAfterCommit() {
        var once = new AtomicBoolean();
        return (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, args) -> {
                    try {
                        Object result = method.invoke(pool, args);
                        if (!(result instanceof Connection connection) || !method.getName().equals("getConnection")) return result;
                        return Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class},
                                (connectionProxy, connectionMethod, connectionArgs) -> {
                                    try {
                                        Object value = connectionMethod.invoke(connection, connectionArgs);
                                        if (connectionMethod.getName().equals("commit") && once.compareAndSet(false, true))
                                            throw new SQLException("Injected post-commit disconnect");
                                        return value;
                                    } catch (InvocationTargetException failure) { throw failure.getCause(); }
                                });
                    } catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
    }
}
