package com.slg.game.application.player;

import com.slg.game.application.port.StorageAccessException;
import com.slg.game.application.read.ReadRejectedException;
import com.slg.game.domain.persistence.RealmIdentity;
import com.slg.game.domain.player.PlayerSnapshot;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;

/**
 * 有界不可变玩家目录，只管理驻留和加载合并，不提供第二个玩家邮箱。业务仍由同一
 * OperationExecutor 取得 Player/请求资格；pin 只防驱逐，不能替代读写顺序或授予写权限。
 * 外层把 Lease 绑到不可取消的内部操作终态，UNKNOWN/发布修复未结束时自然保持 pin。
 *
 * <p>元数据修改在短锁内；loader、计量器、future 完成和所有调用方回调均在锁外。
 * 失效不释放正在执行的加载预算，旧 generation 不得覆盖新发布/重载。目录容量不足
 * 明确失败，不能丢掉 pinned 玩家，也不能把失败伪装成无角色并发放初始资源。</p>
 */
public final class PlayerDirectory {
    public static final long ENTRY_METADATA_BYTES = 256;
    private final Object gate = new Object();
    private final RealmIdentity identity;
    private final PlayerSnapshotLoader loader;
    private final PlayerCacheLimits limits;
    private final ToLongFunction<PlayerSnapshot> sizer;
    private final LongSupplier nanoTime;
    private final Map<Long, Entry> entries = new HashMap<>();
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private final CompletionStage<Void> drainView = drained.minimalCompletionStage();
    private long chargedBytes;
    private int pins;
    private int loads;
    private boolean closing;
    private boolean closed;

    public PlayerDirectory(RealmIdentity identity, PlayerSnapshotLoader loader, PlayerCacheLimits limits,
                           ToLongFunction<PlayerSnapshot> sizer) {
        this(identity, loader, limits, sizer, System::nanoTime);
    }

    PlayerDirectory(RealmIdentity identity, PlayerSnapshotLoader loader, PlayerCacheLimits limits,
                    ToLongFunction<PlayerSnapshot> sizer, LongSupplier nanoTime) {
        this.identity = Objects.requireNonNull(identity, "identity");
        this.loader = Objects.requireNonNull(loader, "loader");
        this.limits = Objects.requireNonNull(limits, "limits");
        this.sizer = Objects.requireNonNull(sizer, "sizer");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
    }

    /** pin 与业务执行资格不同；全满时拒绝，不等待其它玩家释放，也不启动加载或调用外部代码。 */
    public Lease pin(long accountId) {
        checkAccount(accountId);
        synchronized (gate) {
            if (closing) throw failure(PlayerCacheException.Reason.CLOSED, "Player directory is draining");
            if (pins >= limits.maxPins()) throw capacity();
            Entry entry = entries.get(accountId);
            if (entry == null) {
                makeRoom(ENTRY_METADATA_BYTES, true, null);
                entry = new Entry(accountId, nanoTime.getAsLong());
                entries.put(accountId, entry);
                chargedBytes += ENTRY_METADATA_BYTES;
            }
            entry.pins++; pins++;
            entry.lastAccess = nanoTime.getAsLong();
            return new Lease(entry);
        }
    }

    /** 同账号同 generation 共用内部加载；外部取消只影响等待，不能取消真实读取或释放预留。 */
    public CompletionStage<PlayerSnapshot> load(long accountId) {
        checkAccount(accountId);
        Load attempt;
        PlayerSnapshot cached;
        synchronized (gate) {
            Entry entry = pinned(accountId);
            entry.lastAccess = nanoTime.getAsLong();
            cached = entry.snapshot;
            if (cached != null) attempt = null;
            else if (entry.current != null && entry.current.generation == entry.generation) return entry.current.view;
            else {
                makeRoom(limits.maxSnapshotBytes(), false, entry);
                attempt = new Load(entry, entry.generation);
                entry.current = attempt;
                entry.loads++; loads++;
                chargedBytes += limits.maxSnapshotBytes();
            }
        }
        if (cached != null) return CompletableFuture.completedFuture(cached).minimalCompletionStage();
        // 发布加载占位之后再调用 loader，允许并发请求合并，但不能在目录锁内执行 I/O 或回调。
        try {
            Objects.requireNonNull(loader.load(accountId), "loader stage").whenComplete((value, error) -> loaded(attempt, value, error));
        } catch (Throwable error) { loaded(attempt, null, error); }
        return attempt.view;
    }

    /** 失效只撤销可见快照和旧加载的发布资格；其真实 I/O、回调与字节预留仍需正常收敛。 */
    public void invalidate(long accountId) {
        checkAccount(accountId);
        synchronized (gate) {
            Entry entry = entries.get(accountId);
            if (entry == null) return;
            nextGeneration(entry);
            chargedBytes -= entry.snapshotCharge;
            entry.snapshot = null; entry.snapshotCharge = 0; entry.cover = null; entry.current = null;
            entry.lastAccess = nanoTime.getAsLong();
        }
    }

    /** 仅用于持资格且已确认提交的完整后像；容量/身份/水位失败交给外层发布隔离与重载。 */
    public void publish(PlayerSnapshot snapshot) {
        long bytes = measure(snapshot);
        synchronized (gate) {
            Entry entry = pinned(snapshot.accountId());
            verifyProgress(entry, snapshot);
            long increase = Math.max(0, bytes - entry.snapshotCharge);
            makeRoom(increase, false, entry);
            nextGeneration(entry);
            chargedBytes += bytes - entry.snapshotCharge;
            entry.snapshot = snapshot; entry.snapshotCharge = bytes; entry.cover = null; entry.current = null;
            entry.lastAcceptedSequence = snapshot.highSequence();
            entry.lastAccess = nanoTime.getAsLong();
        }
    }

    public int evictIdle() {
        synchronized (gate) {
            int removed = 0;
            long now = nanoTime.getAsLong();
            for (Entry entry : entries.values().toArray(Entry[]::new))
                if (evictable(entry) && now - entry.lastAccess >= limits.idleTtl().toNanos()) { evict(entry); removed++; }
            return removed;
        }
    }

    /** 停止新 pin；已持 pin 的操作仍能加载/修复。所有租用和真实加载/通知结束后才清目录。 */
    public CompletionStage<Void> drain() {
        boolean finish;
        synchronized (gate) { closing = true; finish = finishDrain(); }
        if (finish) drained.complete(null);
        return drainView;
    }

    public Statistics statistics() {
        synchronized (gate) {
            return new Statistics(entries.size(), pins, loads, chargedBytes, closing, closed);
        }
    }

    public record Statistics(int entries, int pins, int inFlightLoads, long chargedBytes, boolean closing, boolean closed) { }

    public final class Lease implements AutoCloseable {
        private final Entry entry;
        private boolean released;
        private Lease(Entry entry) { this.entry = entry; }
        public long accountId() { return entry.account; }
        @Override public void close() {
            boolean finish;
            synchronized (gate) {
                if (released) return;
                released = true;
                entry.pins--; pins--;
                entry.lastAccess = nanoTime.getAsLong();
                finish = finishDrain();
            }
            if (finish) drained.complete(null);
        }
    }

    private void loaded(Load attempt, PlayerSnapshot snapshot, Throwable error) {
        long bytes = 0;
        Throwable resultError = loadFailure(error);
        if (resultError == null) {
            try { bytes = measure(snapshot); }
            catch (Throwable invalid) { resultError = invalidData(); }
        }
        boolean duplicate;
        synchronized (gate) {
            duplicate = attempt.delivering;
            if (!duplicate) attempt.delivering = true;
            if (!duplicate && resultError == null) {
                Entry entry = attempt.entry;
                if (entry.generation != attempt.generation || entry.current != attempt)
                    resultError = failure(PlayerCacheException.Reason.SUPERSEDED, "Player load was invalidated before publication");
                else {
                    try {
                        if (snapshot.accountId() != entry.account) throw new IllegalArgumentException("Account mismatch");
                        verifyProgress(entry, snapshot);
                        // 同一不可变对象由加载预留暂时覆盖驻留计量，通知结束才把 max 预留转成实际
                        // 大小。期间 invalidate/publish 若替换它，旧预留仍保留，不能借此超卖容量。
                        chargedBytes -= entry.snapshotCharge;
                        entry.snapshot = snapshot; entry.snapshotCharge = 0; entry.cover = attempt;
                        entry.lastAcceptedSequence = snapshot.highSequence();
                        entry.lastAccess = nanoTime.getAsLong();
                        attempt.installedBytes = bytes;
                    } catch (Throwable invalid) { resultError = invalidData(); }
                }
            }
        }
        if (duplicate) return;
        try {
            if (resultError == null) attempt.result.complete(snapshot);
            else attempt.result.completeExceptionally(resultError);
        } finally {
            boolean finish;
            synchronized (gate) {
                Entry entry = attempt.entry;
                if (entry.cover == attempt) {
                    entry.snapshotCharge = attempt.installedBytes;
                    chargedBytes += attempt.installedBytes;
                    entry.cover = null;
                }
                chargedBytes -= limits.maxSnapshotBytes();
                entry.loads--; loads--;
                if (entry.current == attempt) entry.current = null;
                finish = finishDrain();
            }
            if (finish) drained.complete(null);
        }
    }

    private long measure(PlayerSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        if (!identity.equals(snapshot.identity())) throw new IllegalArgumentException("Player snapshot dataset differs");
        long measured = sizer.applyAsLong(snapshot);
        if (measured <= 0 || measured > limits.maxSnapshotBytes())
            throw new IllegalArgumentException("Player snapshot exceeds configured byte limit");
        return measured;
    }

    private void verifyProgress(Entry entry, PlayerSnapshot snapshot) {
        if (snapshot.highSequence() < entry.lastAcceptedSequence)
            throw new IllegalArgumentException("Player snapshot watermark moved backwards");
        if (entry.snapshot != null && snapshot.highSequence() == entry.snapshot.highSequence() && !snapshot.equals(entry.snapshot))
            throw new IllegalArgumentException("Different player states share one committed watermark");
    }

    private Entry pinned(long accountId) {
        if (closed) throw failure(PlayerCacheException.Reason.CLOSED, "Player directory has drained");
        Entry entry = entries.get(accountId);
        if (entry == null || entry.pins == 0) throw failure(PlayerCacheException.Reason.NOT_PINNED, "Player must be pinned before access");
        return entry;
    }

    private void makeRoom(long additional, boolean newEntry, Entry protectedEntry) {
        while ((newEntry && entries.size() >= limits.maxEntries()) || additional > limits.maxBytes() - chargedBytes) {
            Entry oldest = null;
            for (Entry candidate : entries.values())
                if (candidate != protectedEntry && evictable(candidate)
                        && (oldest == null || candidate.lastAccess < oldest.lastAccess)) oldest = candidate;
            if (oldest == null) throw capacity();
            evict(oldest);
        }
    }

    private boolean evictable(Entry entry) { return entry.pins == 0 && entry.loads == 0; }
    private void evict(Entry entry) {
        entries.remove(entry.account);
        chargedBytes -= ENTRY_METADATA_BYTES + entry.snapshotCharge;
        entry.snapshot = null;
    }

    private boolean finishDrain() {
        if (!closing || closed || pins != 0 || loads != 0) return false;
        for (Entry entry : entries.values()) entry.snapshot = null;
        entries.clear(); chargedBytes = 0; closed = true;
        return true;
    }

    private static void nextGeneration(Entry entry) {
        if (entry.generation == Long.MAX_VALUE) throw new IllegalStateException("Player cache generation exhausted");
        entry.generation++;
    }
    private static void checkAccount(long accountId) {
        if (accountId <= 0) throw new IllegalArgumentException("Account ID must be positive");
    }
    private static PlayerCacheException capacity() { return failure(PlayerCacheException.Reason.CAPACITY, "Player cache capacity exhausted"); }
    private static PlayerCacheException failure(PlayerCacheException.Reason reason, String message) { return new PlayerCacheException(reason, message); }
    private static StorageAccessException invalidData() {
        return new StorageAccessException(StorageAccessException.Code.INVALID_DATA, "Player cache received an invalid or regressing snapshot");
    }
    private static Throwable unwrap(Throwable error) {
        while (error instanceof CompletionException && error.getCause() != null) error = error.getCause();
        return error;
    }
    private static Throwable loadFailure(Throwable error) {
        error = unwrap(error);
        if (error == null || error instanceof StorageAccessException || error instanceof PlayerCacheException) return error;
        if (error instanceof ReadRejectedException rejected)
            return failure(rejected.reason() == ReadRejectedException.Reason.CAPACITY
                    ? PlayerCacheException.Reason.CAPACITY : PlayerCacheException.Reason.CLOSED,
                    "Player cold-read admission is unavailable");
        // 不把任意适配器异常当成无角色，也不把驱动/连接信息透传到游戏响应。
        return new StorageAccessException(StorageAccessException.Code.UNAVAILABLE, "Player cold read failed");
    }

    private static final class Entry {
        private final long account;
        private int pins;
        private int loads;
        private long generation;
        private long lastAccess;
        private long lastAcceptedSequence;
        private long snapshotCharge;
        private PlayerSnapshot snapshot;
        private Load current;
        private Load cover;
        private Entry(long account, long now) { this.account = account; lastAccess = now; }
    }
    private static final class Load {
        private final Entry entry;
        private final long generation;
        private final CompletableFuture<PlayerSnapshot> result = new CompletableFuture<>();
        private final CompletionStage<PlayerSnapshot> view = result.minimalCompletionStage();
        private boolean delivering;
        private long installedBytes;
        private Load(Entry entry, long generation) { this.entry = entry; this.generation = generation; }
    }
}
