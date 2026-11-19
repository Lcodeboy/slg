package com.slg.game.infrastructure.mysql;

import com.slg.game.application.port.RealmOwnership;
import com.slg.game.application.port.StorageAccessException;
import com.slg.game.domain.persistence.RealmIdentity;
import com.slg.game.domain.persistence.WriterToken;
import com.slg.game.infrastructure.mysql.mapper.RealmMetadataMapper;
import com.slg.game.infrastructure.mysql.metrics.MySqlStorageMetrics;
import com.slg.game.infrastructure.mysql.metrics.MySqlStorageMetrics.Operation;
import com.slg.game.infrastructure.mysql.row.RealmMetadata;
import com.slg.game.infrastructure.mysql.session.MySqlSessions;
import com.slg.game.infrastructure.mysql.session.MySqlErrors;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import org.apache.ibatis.session.SqlSession;

/**
 * 通过数据库 owner 行的排他屏障换任期，等待持共享锁的旧业务全部结束。租约只决定新
 * 事务能否开始，不取消已受 owner 锁保护的事务；所有权操作始终 owner → lease。
 * COMMIT 确认丢失时抛错，绝不根据本地计算的新 epoch 宣称成功或自动重试。
 */
public final class MySqlRealmOwnership implements RealmOwnership {
    private final MySqlSessions sessions;
    private final long leaseMillis;
    private final MySqlStorageMetrics metrics;

    public MySqlRealmOwnership(MySqlSessions sessions, Duration leaseDuration) {
        this(sessions, leaseDuration, new MySqlStorageMetrics());
    }

    public MySqlRealmOwnership(MySqlSessions sessions, Duration leaseDuration, MySqlStorageMetrics metrics) {
        this.sessions = Objects.requireNonNull(sessions, "sessions");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        Objects.requireNonNull(leaseDuration, "leaseDuration");
        this.leaseMillis = leaseDuration.toMillis();
        if (leaseMillis <= 0 || leaseMillis > Integer.MAX_VALUE)
            throw new IllegalArgumentException("Lease duration must be 1..2147483647 milliseconds");
    }

    @Override public OwnershipObservation observe(RealmIdentity identity) throws StorageAccessException {
        return measure(Operation.OWNERSHIP_OBSERVE, () -> observeInternal(identity));
    }

    private OwnershipObservation observeInternal(RealmIdentity identity) throws StorageAccessException {
        Objects.requireNonNull(identity, "identity");
        try (var session = sessions.openRead()) {
            var mapper = session.getMapper(RealmMetadataMapper.class);
            // owner 锁阻止换任期，lease 当前读与续期短暂互斥，使这两个控制行来自同一有效状态。
            // 只观察，不 COMMIT 任何写入；释放锁后结论可变，因此绝不在这里构造可用 token。
            var owner = lockOwnerShared(mapper, identity.realmId());
            sessions.validateIdentity(session, identity);
            requireOwner(owner);
            var lease = requireLease(mapper.lockLeaseShared(identity.realmId()), owner);
            UUID instance = owner.ownerInstance() == null ? null : UUID.fromString(owner.ownerInstance());
            return new OwnershipObservation(identity, owner.writerEpoch(), instance, owner.active(),
                    owner.active() && unexpired(lease));
        } catch (IllegalArgumentException invalid) {
            throw invalid("Invalid realm ownership observation");
        } catch (RuntimeException failure) { throw MySqlErrors.access(failure); }
    }

    @Override public WriterToken acquire(RealmIdentity identity, UUID instanceId) throws StorageAccessException {
        return measure(Operation.OWNERSHIP_ACQUIRE, () -> acquireInternal(identity, instanceId));
    }

    private WriterToken acquireInternal(RealmIdentity identity, UUID instanceId) throws StorageAccessException {
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(instanceId, "instanceId");
        return execute(identity, (session, mapper) -> {
            RealmMetadata.Owner owner = mapper.lockOwnerExclusive(identity.realmId());
            sessions.validateIdentity(session, identity);
            requireOwner(owner);
            RealmMetadata.Lease lease = requireLease(mapper.lockLease(identity.realmId()), owner);
            // 同实例仍不得复用有效写权；这也防止未知 acquire 被盲重试后覆盖自身的新 token。
            if (owner.active() && unexpired(lease)) throw conflict("A live writer already owns this realm");
            long next = nextEpoch(owner.writerEpoch());
            replace(mapper, identity.realmId(), owner, next, instanceId.toString(), true, leaseMillis);
            return new WriterToken(identity, instanceId, next);
        });
    }

    @Override public WriterToken reacquire(OwnershipObservation expected) throws StorageAccessException {
        return measure(Operation.OWNERSHIP_REACQUIRE, () -> reacquireInternal(expected));
    }

    private WriterToken reacquireInternal(OwnershipObservation expected) throws StorageAccessException {
        Objects.requireNonNull(expected, "expected");
        if (!expected.active() || expected.instanceId() == null || expected.leaseValid())
            throw new IllegalArgumentException("Recovery requires an expired owner observation with instance identity");
        return execute(expected.realm(), (session, mapper) -> {
            var owner = mapper.lockOwnerExclusive(expected.realm().realmId());
            sessions.validateIdentity(session, expected.realm());
            requireOwner(owner);
            // 把观察条件的比较放在真正换任期的排他屏障内，关闭 observe→acquire 的竞争窗口。
            // foreign 即使已取得又释放、清空 instance，也不能被本实例当作原租约继续恢复。
            if (!owner.active() || owner.writerEpoch() != expected.epoch()
                    || !expected.instanceId().toString().equalsIgnoreCase(owner.ownerInstance()))
                throw conflict("Observed owner changed before recovery");
            var lease = requireLease(mapper.lockLease(expected.realm().realmId()), owner);
            if (unexpired(lease)) throw conflict("Observed owner lease is still valid");
            long next = nextEpoch(owner.writerEpoch());
            replace(mapper, expected.realm().realmId(), owner, next, expected.instanceId().toString(), true, leaseMillis);
            return new WriterToken(expected.realm(), expected.instanceId(), next);
        });
    }

    @Override public WriterToken fence(WriterToken previous) throws StorageAccessException {
        return measure(Operation.OWNERSHIP_FENCE, () -> fenceInternal(previous));
    }

    private WriterToken fenceInternal(WriterToken previous) throws StorageAccessException {
        Objects.requireNonNull(previous, "previous");
        return execute(previous.realm(), (session, mapper) -> {
            RealmMetadata.Owner owner = mapper.lockOwnerExclusive(previous.realm().realmId());
            sessions.validateIdentity(session, previous.realm());
            requireOwner(owner);
            requireToken(owner, previous);
            requireLease(mapper.lockLease(previous.realm().realmId()), owner);
            // fence 可隔离已经到期但尚未被替换的同一 token；排他锁和完整 CAS 才是证明。
            long next = nextEpoch(owner.writerEpoch());
            replace(mapper, previous.realm().realmId(), owner, next, previous.instanceId().toString(), true, leaseMillis);
            return new WriterToken(previous.realm(), previous.instanceId(), next);
        });
    }

    @Override public boolean renew(WriterToken token) throws StorageAccessException {
        return measure(Operation.OWNERSHIP_RENEW, () -> renewInternal(token));
    }

    private boolean renewInternal(WriterToken token) throws StorageAccessException {
        Objects.requireNonNull(token, "token");
        return execute(token.realm(), (session, mapper) -> {
            RealmMetadata.Owner owner = lockOwnerShared(mapper, token.realm().realmId());
            sessions.validateIdentity(session, token.realm());
            requireOwner(owner);
            if (!matches(owner, token)) return false;
            RealmMetadata.Lease lease = requireLease(mapper.lockLease(token.realm().realmId()), owner);
            // 已过期的 token 必须重新 acquire；心跳不能把失效任期复活。
            if (!unexpired(lease)) return false;
            return mapper.renewLease(token.realm().realmId(), token.epoch(), token.instanceId().toString(), leaseMillis) == 1;
        });
    }

    @Override public void release(WriterToken token) throws StorageAccessException {
        measure(Operation.OWNERSHIP_RELEASE, () -> { releaseInternal(token); return null; });
    }

    private void releaseInternal(WriterToken token) throws StorageAccessException {
        Objects.requireNonNull(token, "token");
        execute(token.realm(), (session, mapper) -> {
            RealmMetadata.Owner owner = mapper.lockOwnerExclusive(token.realm().realmId());
            sessions.validateIdentity(session, token.realm());
            requireOwner(owner);
            if (!matches(owner, token)) return null;
            requireLease(mapper.lockLease(token.realm().realmId()), owner);
            // 保留已用 epoch，清除有效身份；下次 acquire 必须增加 epoch，旧 token 不会复活。
            replace(mapper, token.realm().realmId(), owner, owner.writerEpoch(), null, false, 0);
            return null;
        });
    }

    private <T> T execute(RealmIdentity identity, OwnershipChange<T> operation) throws StorageAccessException {
        SqlSession session = null;
        boolean confirmed = false;
        try {
            session = sessions.openWrite();
            T result = operation.run(session, session.getMapper(RealmMetadataMapper.class));
            session.commit(true);
            confirmed = true;
            return result;
        } catch (StorageAccessException error) {
            throw error;
        } catch (RuntimeException error) {
            throw new StorageAccessException(StorageAccessException.Code.UNAVAILABLE,
                    "Realm ownership could not be confirmed for realm " + identity.realmId());
        } finally {
            if (session != null) {
                // 确认丢失后 rollback 仅用于释放连接，不被解释为先前 COMMIT 未发生的证明。
                if (!confirmed) try { session.rollback(true); } catch (RuntimeException ignored) { }
                // 已确认 COMMIT 后的 close 失败不反转成功结果；连接池负责淘汰坏连接。
                try { session.close(); } catch (RuntimeException ignored) { }
            }
        }
    }

    private static void replace(RealmMetadataMapper mapper, int realmId, RealmMetadata.Owner owner,
                                long nextEpoch, String nextInstance, boolean active, long leaseMillis)
            throws StorageAccessException {
        expectOne(mapper.updateOwner(realmId, owner.writerEpoch(), owner.ownerInstance(), owner.active(),
                nextEpoch, nextInstance, active));
        expectOne(mapper.updateLease(realmId, nextEpoch, nextInstance, leaseMillis));
    }

    private static RealmMetadata.Owner requireOwner(RealmMetadata.Owner owner) throws StorageAccessException {
        if (owner == null || owner.writerEpoch() < 0
                || owner.active() && (owner.writerEpoch() == 0 || owner.ownerInstance() == null)
                || !owner.active() && owner.ownerInstance() != null)
            throw invalid("Missing or invalid realm owner metadata");
        return owner;
    }

    private static RealmMetadata.Lease requireLease(RealmMetadata.Lease lease, RealmMetadata.Owner owner)
            throws StorageAccessException {
        if (lease == null || lease.writerEpoch() != owner.writerEpoch()
                || !Objects.equals(lease.ownerInstance(), owner.ownerInstance())
                || lease.expiresAtMillis() < 0 || lease.databaseNowMillis() <= 0
                || !owner.active() && lease.expiresAtMillis() != 0
                || owner.active() && lease.expiresAtMillis() == 0)
            throw invalid("Owner and lease metadata disagree");
        return lease;
    }

    private static boolean matches(RealmMetadata.Owner owner, WriterToken token) {
        return owner.active() && owner.writerEpoch() == token.epoch()
                && token.instanceId().toString().equalsIgnoreCase(owner.ownerInstance());
    }

    private static void requireToken(RealmMetadata.Owner owner, WriterToken token) throws StorageAccessException {
        if (!matches(owner, token)) throw conflict("Writer token has been replaced or released");
    }

    private static boolean unexpired(RealmMetadata.Lease lease) {
        return lease.expiresAtMillis() > lease.databaseNowMillis();
    }

    private static long nextEpoch(long previous) throws StorageAccessException {
        if (previous == Long.MAX_VALUE) throw conflict("Writer epoch exhausted; writes must remain stopped");
        return previous + 1;
    }

    private static void expectOne(int affected) throws StorageAccessException {
        if (affected != 1) throw conflict("Realm ownership compare-and-set did not update exactly one row");
    }

    private static StorageAccessException conflict(String message) {
        return new StorageAccessException(StorageAccessException.Code.OWNERSHIP_CONFLICT, message);
    }

    private static StorageAccessException invalid(String message) {
        return new StorageAccessException(StorageAccessException.Code.INVALID_DATA, message);
    }

    private RealmMetadata.Owner lockOwnerShared(RealmMetadataMapper mapper, int realmId) {
        long began = System.nanoTime(); boolean completed = false;
        try { var owner = mapper.lockOwnerShared(realmId); completed = true; return owner; }
        finally { metrics.record(Operation.OWNER_SHARED_SQL, System.nanoTime() - began, completed); }
    }

    private <T> T measure(MySqlStorageMetrics.Operation operation, Observed<T> call) throws StorageAccessException {
        long began = System.nanoTime(); boolean completed = false;
        try {
            T result = call.run();
            completed = !Boolean.FALSE.equals(result); // renew=false 是续租失败，不能计作成功心跳。
            return result;
        } finally { metrics.record(operation, System.nanoTime() - began, completed); }
    }

    @FunctionalInterface private interface Observed<T> { T run() throws StorageAccessException; }

    @FunctionalInterface private interface OwnershipChange<T> {
        T run(SqlSession session, RealmMetadataMapper mapper) throws StorageAccessException;
    }
}
