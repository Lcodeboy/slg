package com.slg.game.infrastructure.mysql;

import com.slg.game.application.port.ReadBudget;
import com.slg.game.application.port.RealmStateReader;
import com.slg.game.application.port.StorageAccessException;
import com.slg.game.domain.persistence.*;
import com.slg.game.infrastructure.mysql.mapper.RealmMetadataMapper;
import com.slg.game.infrastructure.mysql.repository.MySqlStateRepository;
import com.slg.game.infrastructure.mysql.row.StateRows;
import com.slg.game.infrastructure.mysql.session.MySqlErrors;
import com.slg.game.infrastructure.mysql.session.MySqlSessions;
import com.slg.game.infrastructure.mysql.session.ReadMeter;
import org.apache.ibatis.session.SqlSession;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import static com.slg.game.application.port.StorageAccessException.Code.INVALID_DATA;

/**
 * 一份快照只持有一个 RR Session。identity、H、根行与明细均为普通一致读，不能混入锁定当前读。
 * 读取失败后快照作废；已交给消费者的条目仍不得发布为完整地图，调用者必须丢弃整份结果并关闭。
 */
public final class MySqlRealmStateReader implements RealmStateReader {
    private final MySqlSessions sessions;
    private final MySqlStateRepository repository;

    public MySqlRealmStateReader(MySqlSessions sessions, MySqlStateRepository repository) {
        this.sessions = Objects.requireNonNull(sessions, "sessions");
        this.repository = Objects.requireNonNull(repository, "repository");
    }

    @Override public Snapshot openSnapshot(RealmIdentity identity, ReadBudget budget) throws StorageAccessException {
        Objects.requireNonNull(identity, "identity");
        var meter = new ReadMeter(budget);
        SqlSession session = null;
        try {
            session = sessions.openRead();
            sessions.validateIdentity(session, identity);
            var cursor = session.getMapper(RealmMetadataMapper.class).cursor(identity.realmId());
            if (cursor == null || cursor.highSeq() < 0 || cursor.prunedThrough() < 0 || cursor.prunedThrough() > cursor.highSeq())
                throw new StorageAccessException(INVALID_DATA, "Invalid authority snapshot cursor");
            meter.add(2, 64);
            meter.checkTime();
            return new View(session, identity, cursor.highSeq(), meter);
        } catch (StorageAccessException | RuntimeException failure) {
            if (session != null) try { session.close(); } catch (RuntimeException closeFailure) { failure.addSuppressed(closeFailure); }
            if (failure instanceof StorageAccessException access) throw access;
            throw MySqlErrors.access(failure);
        }
    }

    private final class View implements Snapshot {
        private final SqlSession session;
        private final RealmIdentity identity;
        private final long highSequence;
        private final ReadMeter meter;
        private final Thread thread = Thread.currentThread();
        private boolean closed;
        private boolean failed;

        View(SqlSession session, RealmIdentity identity, long highSequence, ReadMeter meter) {
            this.session = session;
            this.identity = identity;
            this.highSequence = highSequence;
            this.meter = meter;
        }

        @Override public RealmIdentity identity() { ensureUsable(); return identity; }
        @Override public long highSequence() { ensureUsable(); return highSequence; }

        @Override public Optional<VersionedState> find(ObjectKey key) throws StorageAccessException {
            Objects.requireNonNull(key, "key");
            return read(() -> repository.find(session, identity.realmId(), key, meter, highSequence));
        }

        @Override public Optional<VersionedState> findPlayerByAccount(long accountId) throws StorageAccessException {
            if (accountId <= 0) throw new IllegalArgumentException("Account ID must be positive");
            return read(() -> {
                var query = new StateRows.Query(identity.realmId(), null, null, null, false, null, accountId,
                        null, null, null, false, 2, meter.remainingBytes());
                var rows = repository.roots(session, ObjectKind.PLAYER, query);
                if (rows.isEmpty()) return Optional.empty();
                if (rows.size() != 1) throw new StorageAccessException(INVALID_DATA, "Duplicate account role ownership");
                var loaded = repository.load(session, identity.realmId(), rows.getFirst(), meter);
                requireWithinSnapshot(loaded);
                meter.add(loaded.rows(), loaded.bytes());
                return Optional.of(loaded.state());
            });
        }

        @Override public void scan(ObjectKind kind, Consumer<StateEntry> consumer) throws StorageAccessException {
            scanRows(Objects.requireNonNull(kind, "kind"), null, false, null, consumer);
        }

        @Override public void scanOwned(ObjectKind kind, String playerId, Consumer<StateEntry> consumer) throws StorageAccessException {
            new ObjectKey.Player(playerId);
            scanRows(Objects.requireNonNull(kind, "kind"), playerId, false, null, consumer);
        }

        @Override public void scanActiveMarches(String playerId, Consumer<StateEntry> consumer) throws StorageAccessException {
            new ObjectKey.Player(playerId);
            scanRows(ObjectKind.MARCH, playerId, true, null, consumer);
        }

        @Override public void scanMovingMarches(Consumer<StateEntry> consumer) throws StorageAccessException {
            // 两段共享当前 Session/H/meter。每次 scanRows 的局部游标从 null 开始；不能把第一段
            // 最后的 marchId 带入第二段，否则返程里较小的 ID 会被永久漏掉。
            scanRows(ObjectKind.MARCH, null, false, MarchState.Status.OUTBOUND.code(), consumer);
            scanRows(ObjectKind.MARCH, null, false, MarchState.Status.RETURNING.code(), consumer);
        }

        private void scanRows(ObjectKind kind, String owner, boolean active, Integer movingStatus, Consumer<StateEntry> consumer)
                throws StorageAccessException {
            Objects.requireNonNull(consumer, "consumer");
            ensureUsable();
            ObjectKey after = null;
            while (true) {
                // BLOB 根每页只读一个，防止多行各自合法但一页总载荷远大于本次字节预算。
                int limit = kind == ObjectKind.BATTLE ? 1 : Math.max(1, Math.min(128, meter.remainingRows()));
                var shape = movingStatus != null ? StateRows.Query.moving(identity.realmId(), after, movingStatus, limit)
                        : StateRows.Query.scan(identity.realmId(), after, owner, active, limit);
                var query = shape.withPayloadBudget(meter.remainingBytes());
                var roots = read(() -> repository.roots(session, kind, query));
                if (roots.isEmpty()) return;
                // 预记本页全部根行，后续明细不可占用已取回但尚未组装的根行预算。
                read(() -> { meter.add(roots.size(), 0); return null; });
                for (var row : roots) {
                    var loaded = read(() -> {
                        var item = repository.load(session, identity.realmId(), row, meter, true);
                        requireWithinSnapshot(item);
                        meter.add(item.rows() - 1, item.bytes());
                        return item;
                    });
                    var key = loaded.state().state().key();
                    if (after != null && after.compareTo(key) >= 0) {
                        failed = true;
                        throw new StorageAccessException(INVALID_DATA, "Authority scan is not strictly ordered");
                    }
                    // 消费者异常原样传播，不把业务异常误归类为数据库断连；此快照也不能继续发布。
                    try { consumer.accept(new StateEntry(key, loaded.state())); }
                    catch (RuntimeException failure) { failed = true; throw failure; }
                    // 最后一条回调也属于快照生命周期；不能因下一页为空而漏掉超时/取消检查。
                    read(() -> null);
                    after = key;
                }
                if (roots.size() < limit) return;
            }
        }

        private <T> T read(ReadAction<T> action) throws StorageAccessException {
            ensureUsable();
            try {
                meter.checkTime();
                T value = action.run();
                meter.checkTime();
                return value;
            } catch (StorageAccessException failure) { failed = true; throw failure; }
            catch (RuntimeException failure) { failed = true; throw MySqlErrors.access(failure); }
        }

        private void requireWithinSnapshot(MySqlStateRepository.Loaded loaded) throws StorageAccessException {
            if (loaded.lastSequence() > highSequence)
                throw new StorageAccessException(INVALID_DATA, "Authority state is beyond snapshot watermark");
        }

        private void ensureThread() {
            if (Thread.currentThread() != thread) throw new IllegalStateException("Snapshot belongs to its creating thread");
        }
        private void ensureUsable() {
            ensureThread();
            if (closed || failed) throw new IllegalStateException("Snapshot is closed or has failed");
        }

        @Override public void close() throws StorageAccessException {
            ensureThread();
            if (closed) return;
            closed = true;
            try { session.close(); } catch (RuntimeException failure) { throw MySqlErrors.access(failure); }
        }
    }

    @FunctionalInterface private interface ReadAction<T> { T run() throws StorageAccessException; }
}
