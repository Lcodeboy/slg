package com.slg.game.infrastructure.mysql;

import com.slg.core.economy.ResourceType;
import com.slg.game.application.commit.*;
import com.slg.game.application.port.*;
import com.slg.game.domain.persistence.*;
import com.slg.game.infrastructure.mysql.codec.PersistenceCodec;
import com.slg.game.infrastructure.mysql.mapper.StateMapper;
import com.slg.game.infrastructure.mysql.repository.MySqlStateRepository;
import com.slg.game.infrastructure.mysql.session.MySqlSessions;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.*;

/** 真 MySQL 产生1213/1205，再验证明确回滚不变成UNKNOWN、不换epoch、不自动重投业务。 */
@Timeout(45)
class MySqlTransactionRollbackIT extends AuthoritySchemaFixture {
    private final PersistenceCodec codec = new PersistenceCodec();

    @Test void missingUniqueKeysCanDeadlockAndConfirmedVictimIsRetriedOnlyByTheCaller() throws Exception {
        var ownership = new MySqlRealmOwnership(sessions, Duration.ofSeconds(30));
        var writer = ownership.acquire(identity, UUID.randomUUID());
        var reached = new AtomicInteger(); var deadlocks = new AtomicInteger();
        // 两个缺失唯一键位于空主键索引的同一gap。双方先完成FOR UPDATE再一起INSERT，
        // 会形成真实的insert-intention/gap等待环；包装器只同步执行时点，不伪造1213。
        var controlled = new MySqlSessions(controlledConnections(new CyclicBarrier(2), reached, deadlocks, null, false), 5);
        var actual = transaction(controlled); var unknownAttempts = new AtomicInteger();
        var service = service(writer, recordUnknown(actual, unknownAttempts), ownership);
        var requests = List.of(request(writer, "create-a", new ResourceState("1_101", ResourceType.WOOD, 100), 0),
                request(writer, "create-b", new ResourceState("1_102", ResourceType.WOOD, 200), 0));
        try {
            var handles = requests.stream().map(service::submit).toList();
            var results = List.of(result(handles.get(0)), result(handles.get(1)));
            assertEquals(2, reached.get()); assertTrue(deadlocks.get() >= 1, "MySQL must actually report a deadlock victim");
            assertTrue(results.stream().anyMatch(value -> value instanceof CommitResult.NotCommitted));
            assertEquals(0, unknownAttempts.get()); assertEquals(0, service.snapshot().reconciling());
            assertEquals(writer.epoch(), scalar("SELECT writer_epoch FROM realm_owner WHERE realm_id=1"));
            for (int i = 0; i < results.size(); i++) {
                if (results.get(i) instanceof CommitResult.NotCommitted rolledBack) {
                    assertEquals(CommitResult.Reason.ROLLED_BACK, rolledBack.reason());
                    // 原请求没有回执、没有占序号；显式重试同一决定仍使用原epoch和expectedVersion=0。
                    assertInstanceOf(CommitResult.Committed.class, result(service.submit(requests.get(i))));
                } else assertInstanceOf(CommitResult.Committed.class, results.get(i));
            }
            assertEquals(2, scalar("SELECT COUNT(*) FROM game_player_resource"));
            assertEquals(2, scalar("SELECT COUNT(*) FROM game_receipt"));
            assertEquals(2, scalar("SELECT COUNT(*) FROM game_journal"));
            assertEquals(2, scalar("SELECT high_seq FROM realm_commit_cursor WHERE realm_id=1"));
            assertEquals(writer.epoch(), scalar("SELECT writer_epoch FROM realm_owner WHERE realm_id=1"));
        } finally { drain(service); }
    }

    @Test void realLockWaitTimeoutAfterConfirmedRollbackDoesNotFenceTheWriter() throws Exception {
        var ownership = new MySqlRealmOwnership(sessions, Duration.ofSeconds(30));
        var writer = ownership.acquire(identity, UUID.randomUUID());
        var initial = new ResourceState("1_7", ResourceType.WOOD, 0);
        assertInstanceOf(RealmTransaction.CommitAttempt.Settled.class,
                transaction(sessions).commit(writer, request(writer, "seed", initial, 0).decision()));
        var timeouts = new AtomicInteger();
        var timed = new MySqlSessions(controlledConnections(null, new AtomicInteger(), null, timeouts, true), 5);
        var service = service(writer, transaction(timed), ownership);
        var update = request(writer, "timed-update", new ResourceState("1_7", ResourceType.WOOD, 100), 1);
        try {
            try (var blocking = sessions.openWrite()) {
                assertNotNull(blocking.getMapper(StateMapper.class).lockVersion(identity.realmId(), initial.key()));
                var failure = assertInstanceOf(CommitResult.NotCommitted.class, result(service.submit(update)));
                assertEquals(CommitResult.Reason.ROLLED_BACK, failure.reason());
                assertEquals(1, timeouts.get()); assertEquals(0, service.snapshot().reconciling());
                assertEquals(writer.epoch(), scalar("SELECT writer_epoch FROM realm_owner WHERE realm_id=1"));
                assertEquals(1, scalar("SELECT high_seq FROM realm_commit_cursor WHERE realm_id=1"));
                blocking.rollback(true);
            }
            assertInstanceOf(CommitResult.Committed.class, result(service.submit(update)));
            assertEquals(100, scalar("SELECT amount FROM game_player_resource WHERE player_id='1_7'"));
            assertEquals(2, scalar("SELECT COUNT(*) FROM game_journal"));
            assertEquals(writer.epoch(), scalar("SELECT writer_epoch FROM realm_owner WHERE realm_id=1"));
        } finally { drain(service); }
    }

    private DataSource controlledConnections(CyclicBarrier afterMissingLock, AtomicInteger reached,
                                             AtomicInteger deadlocks, AtomicInteger timeouts, boolean shortLockTimeout) {
        return (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, args) -> {
                    try {
                        Object value = method.invoke(pool, args);
                        if (!(value instanceof Connection connection)) return value;
                        if (shortLockTimeout) {
                            // 只修改本fixture连接池中的会话；不改数据库全局配置或外部环境。
                            try (var statement = connection.createStatement()) { statement.execute("SET SESSION innodb_lock_wait_timeout=1"); }
                        }
                        return Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class}, (p, call, arguments) -> {
                            try {
                                Object result = call.invoke(connection, arguments);
                                if (!(result instanceof PreparedStatement prepared) || !call.getName().equals("prepareStatement")) return result;
                                String sql = ((String) arguments[0]).toLowerCase(Locale.ROOT);
                                boolean lock = sql.startsWith("select version,last_seq from game_player_resource") && sql.contains("for update");
                                return Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{PreparedStatement.class}, (s, operation, parameters) -> {
                                    try {
                                        Object output = operation.invoke(prepared, parameters);
                                        if (afterMissingLock != null && lock && (operation.getName().equals("execute")
                                                || operation.getName().equals("executeQuery"))) {
                                            int ticket = reached.incrementAndGet();
                                            if (ticket <= 2) try { afterMissingLock.await(3, TimeUnit.SECONDS); }
                                            catch (Exception failure) { throw new SQLException("Controlled lock rendezvous failed", failure); }
                                        }
                                        return output;
                                    } catch (InvocationTargetException failure) {
                                        Throwable cause = failure.getCause();
                                        if (cause instanceof SQLException sqlError) {
                                            if (sqlError.getErrorCode() == 1213 && deadlocks != null) deadlocks.incrementAndGet();
                                            if (sqlError.getErrorCode() == 1205 && timeouts != null) timeouts.incrementAndGet();
                                        }
                                        throw cause;
                                    }
                                });
                            } catch (InvocationTargetException failure) { throw failure.getCause(); }
                        });
                    } catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
    }

    private RealmTransaction recordUnknown(RealmTransaction actual, AtomicInteger unknown) {
        return new RealmTransaction() {
            @Override public CommitAttempt commit(WriterToken writer, CommitDecision decision) throws StorageAccessException {
                var result = actual.commit(writer, decision);
                if (result instanceof CommitAttempt.Uncertain) unknown.incrementAndGet();
                return result;
            }
            @Override public CommitAttempt resolveAfterFence(WriterToken previous, WriterToken replacement, RequestKey request, String digest)
                    throws StorageAccessException { return actual.resolveAfterFence(previous, replacement, request, digest); }
        };
    }
    private MySqlRealmTransaction transaction(MySqlSessions source) {
        return new MySqlRealmTransaction(source, new MySqlStateRepository(codec), codec);
    }
    private BoundedRealmCommitService service(WriterToken writer, RealmTransaction transaction, RealmOwnership ownership) {
        return new BoundedRealmCommitService(writer, transaction, ownership,
                request -> codec.encodeDecision(request.decision()).length + 128L,
                new CommitQueueLimits(8, 1_000_000, 1, 100_000, 2, 2,
                        Duration.ofSeconds(5), Duration.ofMillis(20), Duration.ofSeconds(10)));
    }
    private static CommitRequest request(WriterToken writer, String id, ResourceState state, long version) {
        return new CommitRequest(writer, new CommitDecision(new RequestKey("rollback-it", id), "digest-" + id,
                "ROLLBACK_TEST", "{}", "{\"ok\":true}", List.of(new ObjectChange(state.key(), version, state)), Map.of()),
                CommitRequest.Origin.CLIENT, Duration.ofSeconds(5));
    }
    private static CommitResult result(CommitHandle handle) throws Exception {
        return handle.settlement().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }
    private static void drain(BoundedRealmCommitService service) throws Exception {
        service.drain().toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertEquals(CommitQueueSnapshot.Mode.CLOSED, service.snapshot().mode());
    }
}
