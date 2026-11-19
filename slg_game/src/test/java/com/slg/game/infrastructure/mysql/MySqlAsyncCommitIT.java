package com.slg.game.infrastructure.mysql;

import com.slg.core.economy.ResourceType;
import com.slg.game.application.commit.*;
import com.slg.game.application.port.*;
import com.slg.game.domain.persistence.*;
import com.slg.game.infrastructure.mysql.codec.PersistenceCodec;
import com.slg.game.infrastructure.mysql.repository.MySqlStateRepository;
import com.slg.game.infrastructure.mysql.session.MySqlSessions;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.*;

/** 独立测试库覆盖异步队列与真实事务/owner屏障连接，不启动 GAME 或触碰旧运行链路。 */
@Timeout(45)
class MySqlAsyncCommitIT extends AuthoritySchemaFixture {
    private final PersistenceCodec codec = new PersistenceCodec();

    @Test void independentAsyncDecisionsProduceOneContinuousCommittedPrefix() throws Exception {
        var ownership = new MySqlRealmOwnership(sessions, Duration.ofSeconds(30));
        var writer = ownership.acquire(identity, UUID.randomUUID());
        // 空表上的多个缺失唯一键可落在同一 gap，首次并发 INSERT 允许发生确定回滚。
        // 本例专门验证独立已存在行的并行提交，先用一笔事务准备版本1，再各自更新。
        var seeds = new ArrayList<ObjectChange>();
        for (int i = 1; i <= 8; i++) {
            var resource = new ResourceState("1_" + i, ResourceType.WOOD, 0);
            seeds.add(new ObjectChange(resource.key(), 0, resource));
        }
        assertInstanceOf(CommitResult.Committed.class, assertInstanceOf(RealmTransaction.CommitAttempt.Settled.class,
                transaction(sessions).commit(writer, request(writer, "seed", seeds).decision())).result());
        var service = service(writer, transaction(sessions), ownership);
        try {
            var handles = new ArrayList<CommitHandle>();
            for (int i = 1; i <= 8; i++) {
                var resource = new ResourceState("1_" + i, ResourceType.WOOD, i * 100L);
                handles.add(service.submit(request(writer, "independent-" + i, List.of(new ObjectChange(resource.key(), 1, resource)))));
            }
            for (var handle : handles) assertTrue(committed(handle).applied());
            assertEquals(8, scalar("SELECT COUNT(*) FROM game_player_resource"));
            assertEquals(9, scalar("SELECT high_seq FROM realm_commit_cursor WHERE realm_id=1"));
            var events = new MySqlRealmEventReader(sessions, codec).readAfter(identity, 1, 100,
                    new ReadBudget(100, 1_000_000, Duration.ofSeconds(5)));
            assertEquals(8, events.events().size()); assertEquals(9, events.through());
        } finally { drain(service); }
    }

    @Test void lostBusinessCommitAcknowledgementIsFencedAndReturnedAsOriginalReceipt() throws Exception {
        var ownership = new MySqlRealmOwnership(sessions, Duration.ofSeconds(30));
        var writer = ownership.acquire(identity, UUID.randomUUID());
        var faultyTransaction = transaction(new MySqlSessions(failFirstCommitAcknowledgement(), 5));
        var service = service(writer, faultyTransaction, ownership);
        var wood = new ResourceState("1_7", ResourceType.WOOD, 100);
        try {
            var result = committed(service.submit(request(writer, "lost-commit", List.of(new ObjectChange(wood.key(), 0, wood)))));
            assertFalse(result.applied()); assertEquals(1, result.receipt().sequence());
            assertEquals(100, scalar("SELECT amount FROM game_player_resource WHERE player_id='1_7'"));
            assertEquals(1, scalar("SELECT COUNT(*) FROM game_receipt")); assertEquals(1, scalar("SELECT COUNT(*) FROM game_journal"));
            assertEquals(writer.epoch() + 1, scalar("SELECT writer_epoch FROM realm_owner WHERE realm_id=1"));
        } finally { drain(service); }
    }

    @Test void lostFenceAcknowledgementWaitsForDatabaseLeaseExpiryThenAcquiresANewKnownEpoch() throws Exception {
        var ordinaryOwnership = new MySqlRealmOwnership(sessions, Duration.ofSeconds(30));
        var writer = ordinaryOwnership.acquire(identity, UUID.randomUUID());
        var unreliableOwnership = new MySqlRealmOwnership(new MySqlSessions(failFirstCommitAcknowledgement(), 5), Duration.ofSeconds(30));
        var actual = transaction(sessions); var first = new AtomicBoolean(true);
        RealmTransaction uncertainResponse = new RealmTransaction() {
            @Override public CommitAttempt commit(WriterToken token, CommitDecision decision) throws StorageAccessException {
                var outcome = actual.commit(token, decision);
                return first.compareAndSet(true, false) ? new CommitAttempt.Uncertain() : outcome;
            }
            @Override public CommitAttempt resolveAfterFence(WriterToken previous, WriterToken next, RequestKey key, String digest)
                    throws StorageAccessException { return actual.resolveAfterFence(previous, next, key, digest); }
        };
        var service = service(writer, uncertainResponse, unreliableOwnership);
        try {
            var handle = service.submit(request(writer, "lost-fence", List.of()));
            await(() -> {
                try { return scalar("SELECT writer_epoch FROM realm_owner WHERE realm_id=1") == writer.epoch() + 1; }
                catch (SQLException failure) { throw new AssertionError(failure); }
            });
            assertFalse(handle.settlement().toCompletableFuture().isDone()); assertTrue(service.currentWriter().isEmpty());
            Thread.sleep(100);
            assertEquals(writer.epoch() + 1, scalar("SELECT writer_epoch FROM realm_owner WHERE realm_id=1"));
            // 故障注入只使本测试租约过期，模拟确认丢失后停止续期的数据库时钟结果。
            expireLease();
            var recovered = committed(handle);
            assertFalse(recovered.applied()); assertEquals(1, recovered.receipt().sequence());
            assertEquals(writer.epoch() + 2, scalar("SELECT writer_epoch FROM realm_owner WHERE realm_id=1"));
            assertEquals(1, scalar("SELECT COUNT(*) FROM game_journal"));
        } finally { expireLease(); drain(service); }
    }

    private BoundedRealmCommitService service(WriterToken writer, RealmTransaction transaction, RealmOwnership ownership) {
        var limits = new CommitQueueLimits(16, 4_000_000, 2, 200_000, 3, 2,
                Duration.ofSeconds(10), Duration.ofMillis(20), Duration.ofSeconds(10));
        return new BoundedRealmCommitService(writer, transaction, ownership,
                request -> codec.encodeDecision(request.decision()).length + 128L, limits);
    }
    private MySqlRealmTransaction transaction(MySqlSessions source) {
        return new MySqlRealmTransaction(source, new MySqlStateRepository(codec), codec);
    }
    private static CommitRequest request(WriterToken writer, String id, List<ObjectChange> changes) {
        var decision = new CommitDecision(new RequestKey("async-it", id), "digest-" + id,
                "ASYNC_TEST", "{}", "{\"ok\":true}", changes, Map.of());
        return new CommitRequest(writer, decision, CommitRequest.Origin.CLIENT, Duration.ofSeconds(5));
    }
    private static CommitResult.Committed committed(CommitHandle handle) throws Exception {
        return assertInstanceOf(CommitResult.Committed.class, handle.settlement().toCompletableFuture().get(10, TimeUnit.SECONDS));
    }
    private static void drain(BoundedRealmCommitService service) throws Exception {
        service.drain().toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertEquals(CommitQueueSnapshot.Mode.CLOSED, service.snapshot().mode());
    }
    private static void await(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(10);
        assertTrue(condition.getAsBoolean(), "Timed out waiting for database recovery transition");
    }
    private void expireLease() throws SQLException {
        execute("UPDATE realm_owner_lease SET expires_at_millis="
                + "CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3))*1000 AS SIGNED)-1 WHERE realm_id=1");
    }
    private DataSource failFirstCommitAcknowledgement() {
        var once = new AtomicBoolean();
        return (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, args) -> {
                    try {
                        Object value = method.invoke(pool, args);
                        if (!(value instanceof Connection connection)) return value;
                        return Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class}, (p, call, arguments) -> {
                            try {
                                Object result = call.invoke(connection, arguments);
                                if (call.getName().equals("commit") && once.compareAndSet(false, true))
                                    throw new SQLException("Injected lost commit acknowledgement", "08006");
                                return result;
                            } catch (InvocationTargetException failure) { throw failure.getCause(); }
                        });
                    } catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
    }
}
