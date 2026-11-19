package com.slg.game.infrastructure.mysql;

import com.slg.game.application.commit.CommitResult;
import com.slg.game.application.port.RealmTransaction;
import com.slg.game.application.port.StorageAccessException;
import com.slg.game.domain.persistence.*;
import com.slg.game.infrastructure.mysql.codec.PersistenceCodec;
import com.slg.game.infrastructure.mysql.mapper.JournalMapper;
import com.slg.game.infrastructure.mysql.mapper.RealmMetadataMapper;
import com.slg.game.infrastructure.mysql.metrics.MySqlStorageMetrics;
import com.slg.game.infrastructure.mysql.metrics.MySqlStorageMetrics.Operation;
import com.slg.game.infrastructure.mysql.repository.MySqlStateRepository;
import com.slg.game.infrastructure.mysql.row.JournalRows;
import com.slg.game.infrastructure.mysql.session.MySqlErrors;
import com.slg.game.infrastructure.mysql.session.MySqlSessions;
import java.util.ArrayList;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Supplier;
import org.apache.ibatis.session.SqlSession;

import static com.slg.game.application.commit.CommitResult.Reason.*;

/**
 * 单笔业务的 Unit of Work；只保存已经决定的不可变值，不计算奖励、不操作玩家内存。
 * 调用者必须持冲突对象/玩家/请求资格，并在有界数据库执行器上调用本阻塞端口。
 * SIMPLE 执行使每一步 SQL 在继续取锁之前完成，不能以入批成功代替 CAS 或 COMMIT。
 */
public final class MySqlRealmTransaction implements RealmTransaction {
    private final MySqlSessions sessions;
    private final MySqlStateRepository states;
    private final PersistenceCodec codec;
    private final MySqlStorageMetrics metrics;

    public MySqlRealmTransaction(MySqlSessions sessions, MySqlStateRepository states, PersistenceCodec codec) {
        this(sessions, states, codec, new MySqlStorageMetrics());
    }

    public MySqlRealmTransaction(MySqlSessions sessions, MySqlStateRepository states, PersistenceCodec codec,
                                 MySqlStorageMetrics metrics) {
        this.sessions = Objects.requireNonNull(sessions);
        this.states = Objects.requireNonNull(states);
        this.codec = Objects.requireNonNull(codec);
        this.metrics = Objects.requireNonNull(metrics);
    }

    @Override public CommitAttempt commit(WriterToken writer, CommitDecision decision) throws StorageAccessException {
        long began = System.nanoTime(); boolean committed = false;
        try {
            var result = commitInternal(writer, decision);
            committed = result instanceof CommitAttempt.Settled settled && settled.result() instanceof CommitResult.Committed;
            return result;
        } finally { metrics.record(Operation.COMMIT, System.nanoTime() - began, committed); }
    }

    private CommitAttempt commitInternal(WriterToken writer, CommitDecision decision) throws StorageAccessException {
        Objects.requireNonNull(writer, "writer");
        Objects.requireNonNull(decision, "decision");
        final byte[] payload;
        try {
            payload = codec.encodeDecision(decision);
            for (var change : decision.changes())
                if (change.afterState() instanceof PlayerState player && player.currentRealmId() != writer.realm().realmId())
                    return rejected(INVALID_DECISION);
        } catch (IllegalArgumentException invalid) { return rejected(INVALID_DECISION); }

        SqlSession session = null;
        boolean commitSent = false;
        try {
            session = sessions.openWrite();
            // owner 共享锁始终先取，之后才建立一致读视图核对身份/租约及原回执。
            // 租约是开始事务的准入条件；真正等待在途事务的屏障是这个 owner 行锁。
            if (!lockWriter(session, writer)) return rollbackAndReject(session, OWNER_LOST);
            var journal = session.getMapper(JournalMapper.class);
            var previous = journal.findReceipt(writer.realm().realmId(), decision.request());
            if (previous != null) return replay(receipt(session, writer.realm(), decision.request(), previous), decision.digest());

            var expected = new TreeMap<ObjectKey, Long>(ObjectKey.ORDER);
            expected.putAll(decision.readVersions());
            decision.changes().forEach(change -> expected.put(change.key(), change.expectedVersion()));
            for (var dependency : expected.entrySet()) {
                if (states.lockVersion(session, writer.realm().realmId(), dependency.getKey()) != dependency.getValue()) {
                    if (!rollback(session)) return new CommitAttempt.Uncertain();
                    close(session);
                    session = null;
                    return replayOrReject(writer.realm(), decision, VERSION_CONFLICT);
                }
            }
            var changes = new ArrayList<>(decision.changes());
            changes.sort((left, right) -> left.key().compareTo(right.key()));
            // 根及明细先完成 CAS/INSERT；创建唯一键的冲突在 cursor 之前暴露，避免反向锁等待。
            // 新根的 last_seq=0 只是本事务内占位，下面取序号后逐一最终化；外部永远不应看见 0。
            for (var change : changes) states.write(session, writer.realm().realmId(), change);

            var metadata = session.getMapper(RealmMetadataMapper.class);
            var cursor = sql(Operation.CURSOR_LOCK_SQL, () -> metadata.lockCursor(writer.realm().realmId()));
            if (cursor == null || cursor.highSeq() < 0 || cursor.prunedThrough() < 0
                    || cursor.prunedThrough() > cursor.highSeq() || cursor.highSeq() == Long.MAX_VALUE)
                throw new StorageAccessException(StorageAccessException.Code.INVALID_DATA, "Invalid authority commit cursor");
            long sequence = cursor.highSeq() + 1;
            var receipt = new CommitReceipt(decision.request(), sequence, decision.digest(), decision.resultJson());
            // 取得 cursor 后只更新先前已锁定的根以及本事务的新事件/回执，不再获取新业务对象锁。
            for (var change : changes) states.finalizeSequence(session, writer.realm().realmId(), change.key(), sequence);
            requireOne(journal.insertEvent(writer.realm().realmId(), sequence, 1, payload, codec.hash(payload)));
            requireOne(journal.insertReceipt(writer.realm().realmId(), receipt));
            requireOne(metadata.advanceCursor(writer.realm().realmId(), cursor.highSeq(), sequence));
            commitSent = true;
            session.commit(true);
            return new CommitAttempt.Settled(new CommitResult.Committed(receipt, true));
        } catch (MySqlStateRepository.VersionConflict conflict) {
            if (commitSent || !rollback(session)) return new CommitAttempt.Uncertain();
            close(session);
            session = null;
            return replayOrReject(writer.realm(), decision, VERSION_CONFLICT);
        } catch (StorageAccessException failure) {
            if (commitSent || !rollback(session)) return new CommitAttempt.Uncertain();
            throw failure;
        } catch (RuntimeException failure) {
            // 一旦发出 COMMIT，不用 SQLState、重试或回滚尝试猜测结果；必须经过 owner 屏障核对。
            if (commitSent || !rollback(session)) return new CommitAttempt.Uncertain();
            if (MySqlErrors.constraintConflict(failure)) {
                close(session);
                session = null;
                return replayOrReject(writer.realm(), decision, VERSION_CONFLICT);
            }
            // 锁等待超时只可能撤销单条 SQL，因此必须在上面的整笔 rollback 已确认后才给确定
            // 未提交。常规死锁不应升级为全区 UNKNOWN 屏障；重试仍由用例重新读取/复核决定。
            if (MySqlErrors.lockConflict(failure)) return rejected(ROLLED_BACK);
            throw MySqlErrors.access(failure);
        } finally { close(session); }
    }

    @Override public CommitAttempt resolveAfterFence(WriterToken previous, WriterToken replacement,
                                                     RequestKey request, String digest) throws StorageAccessException {
        long began = System.nanoTime(); boolean settled = false;
        try {
            var result = resolveInternal(previous, replacement, request, digest);
            settled = result instanceof CommitAttempt.Settled;
            return result;
        } finally { metrics.record(Operation.RESOLVE_AFTER_FENCE, System.nanoTime() - began, settled); }
    }

    private CommitAttempt resolveInternal(WriterToken previous, WriterToken replacement,
                                          RequestKey request, String digest) throws StorageAccessException {
        Objects.requireNonNull(previous); Objects.requireNonNull(replacement);
        Objects.requireNonNull(request); Objects.requireNonNull(digest);
        if (!previous.realm().equals(replacement.realm()) || replacement.epoch() <= previous.epoch())
            return new CommitAttempt.Uncertain();
        SqlSession session = null;
        try {
            session = sessions.openRead();
            // 更高且仍有效的 owner 任期来自数据库排他屏障；获取它的共享锁后，旧任期已不可能
            // 迟到提交。调用方仍须独占该请求，禁止 replacement 下的同请求尝试并行执行。
            if (!lockWriter(session, replacement)) return new CommitAttempt.Uncertain();
            var receipt = session.getMapper(JournalMapper.class).findReceipt(replacement.realm().realmId(), request);
            return receipt == null ? rejected(ROLLED_BACK) : replay(receipt(session, replacement.realm(), request, receipt), digest);
        } catch (RuntimeException failure) { throw MySqlErrors.access(failure); }
        finally { close(session); }
    }

    private boolean lockWriter(SqlSession session, WriterToken writer) throws StorageAccessException {
        var metadata = session.getMapper(RealmMetadataMapper.class);
        var owner = sql(Operation.OWNER_SHARED_SQL, () -> metadata.lockOwnerShared(writer.realm().realmId()));
        sessions.validateIdentity(session, writer.realm());
        if (owner == null || !owner.active() || owner.writerEpoch() != writer.epoch()
                || !writer.instanceId().toString().equalsIgnoreCase(owner.ownerInstance())) return false;
        var lease = metadata.lease(writer.realm().realmId());
        return lease != null && lease.writerEpoch() == writer.epoch()
                && writer.instanceId().toString().equalsIgnoreCase(lease.ownerInstance())
                && lease.expiresAtMillis() > lease.databaseNowMillis();
    }

    private CommitAttempt replayOrReject(RealmIdentity identity, CommitDecision decision, CommitResult.Reason reason)
            throws StorageAccessException {
        // 并发同请求可能在本次等待对象锁时提交。回滚后的新视图才能看到它；不能用旧 RR 空读
        // 判定不存在，也不能在持 cursor 时重新申请缺失回执的 gap lock。
        try (var session = sessions.openRead()) {
            sessions.validateIdentity(session, identity);
            var previous = session.getMapper(JournalMapper.class).findReceipt(identity.realmId(), decision.request());
            return previous == null ? rejected(reason) : replay(receipt(session, identity, decision.request(), previous), decision.digest());
        } catch (RuntimeException failure) { throw MySqlErrors.access(failure); }
    }

    private CommitReceipt receipt(SqlSession session, RealmIdentity identity, RequestKey key, JournalRows.Receipt row)
            throws StorageAccessException {
        try {
            var receipt = row.toDomain(codec);
            var cursor = session.getMapper(RealmMetadataMapper.class).cursor(identity.realmId());
            if (!receipt.request().equals(key) || cursor == null || cursor.highSeq() < receipt.sequence()
                    || cursor.prunedThrough() < 0 || cursor.prunedThrough() > cursor.highSeq())
                throw new IllegalArgumentException("Invalid receipt watermark");
            return receipt;
        } catch (IllegalArgumentException invalid) {
            throw new StorageAccessException(StorageAccessException.Code.INVALID_DATA, "Invalid authority receipt");
        }
    }

    private static CommitAttempt replay(CommitReceipt receipt, String digest) {
        return receipt.digest().equals(digest)
                ? new CommitAttempt.Settled(new CommitResult.Committed(receipt, false)) : rejected(REQUEST_CONFLICT);
    }
    private static CommitAttempt rejected(CommitResult.Reason reason) {
        return new CommitAttempt.Settled(new CommitResult.NotCommitted(reason));
    }
    private static CommitAttempt rollbackAndReject(SqlSession session, CommitResult.Reason reason) {
        return rollback(session) ? rejected(reason) : new CommitAttempt.Uncertain();
    }
    private static boolean rollback(SqlSession session) {
        if (session == null) return true;
        try { session.rollback(true); return true; }
        catch (RuntimeException failure) { return false; }
    }
    private static void close(SqlSession session) {
        if (session == null) return;
        try { session.close(); }
        catch (RuntimeException closeFailure) {
            // 已确认 COMMIT 不能被 close 异常改成未提交；池/连接回收诊断不含原始驱动消息。
            System.getLogger(MySqlRealmTransaction.class.getName()).log(System.Logger.Level.WARNING,
                    "Could not confirm authority session cleanup");
        }
    }
    private static void requireOne(int affected) throws StorageAccessException {
        if (affected != 1)
            throw new StorageAccessException(StorageAccessException.Code.INVALID_DATA, "Authority write affected an unexpected number of rows");
    }
    private <T> T sql(Operation operation, Supplier<T> call) {
        long began = System.nanoTime(); boolean completed = false;
        try { T result = call.get(); completed = true; return result; }
        finally { metrics.record(operation, System.nanoTime() - began, completed); }
    }
}
