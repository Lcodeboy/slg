package com.slg.game.application.read;

import com.slg.game.application.port.ReadBudget;
import com.slg.game.application.port.RealmReceiptReader;
import com.slg.game.application.port.RealmStateReader;
import com.slg.game.application.port.RealmWorkReader;
import com.slg.game.application.port.StorageAccessException;
import com.slg.game.domain.persistence.ObjectKey;
import com.slg.game.domain.persistence.ObjectKind;
import com.slg.game.domain.persistence.RealmIdentity;
import com.slg.game.domain.persistence.RequestKey;
import com.slg.game.domain.persistence.VersionedState;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * 独立冷读通道：数量、固定参数元数据、I/O 工作线程与通知位置在准入时一起预留。
 * 真快照只在工作线程打开/使用/关闭；通知线程完成 Future，慢用户回调不占 JDBC 槽位。
 * 任务预算直到通知完成才释放，外部 Future 取消不改变真实任务，也不制造可重复使用的槽。
 *
 * <p>每次输入只有 reader/query 引用、realm/dataset、ReadBudget 和句柄；回执查询另计请求键。
 * 256 是固定调度
 * 元数据的保守计量，另计区号4、UUID16、maxRows4、maxBytes8、Duration12字节；不是 JVM
 * 堆大小的精确承诺，不接受调用者自报长度。读取结果遵循 ReadBudget 和 SnapshotQuery 的
 * 有界不可变结果契约，交付后的应用缓存不归本执行器计量。</p>
 */
public final class BoundedReadExecutor implements AutoCloseable {
    public static final long TASK_METADATA_BYTES = 256L + 4 + 16 + 4 + 8 + 12;
    private static final AtomicLong INSTANCES = new AtomicLong();
    private final Object monitor = new Object();
    private final ReadExecutionLimits limits;
    private final ArrayDeque<Task<?>> work = new ArrayDeque<>();
    private final ArrayDeque<Task<?>> notifications = new ArrayDeque<>();
    private final List<Thread> threads = new ArrayList<>();
    private final List<Notifier> notifiers = new ArrayList<>();
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private boolean accepting = true;
    private boolean drainCompletionClaimed;
    private int outstanding;
    private int executing;
    private long metadataBytes;

    public BoundedReadExecutor(ReadExecutionLimits limits) {
        this.limits = Objects.requireNonNull(limits, "limits");
        String prefix = "slg-cold-read-" + INSTANCES.incrementAndGet();
        for (int i = 0; i < limits.notificationWorkers(); i++) {
            var notifier = new Notifier();
            notifiers.add(notifier);
            threads.add(thread(prefix + "-notify-" + i, () -> notifyLoop(notifier)));
        }
        for (int i = 0; i < limits.workers(); i++) threads.add(thread(prefix + "-worker-" + i, this::workLoop));
        threads.forEach(Thread::start);
    }

    /** 满额立即明确拒绝；不等待连接、队列或通知槽，也不创建任意公共池任务。 */
    public <T> ReadHandle<T> readSnapshot(RealmStateReader reader, RealmIdentity identity, ReadBudget budget,
                                         SnapshotQuery<T> query) {
        Objects.requireNonNull(reader, "reader");
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(budget, "budget");
        Objects.requireNonNull(query, "query");
        return submit(TASK_METADATA_BYTES, () -> {
            try (RealmStateReader.Snapshot snapshot = reader.openSnapshot(identity, budget)) {
                var view = new GuardedView(snapshot);
                try {
                    T value = query.read(view);
                    if (value instanceof SnapshotView || value instanceof RealmStateReader.Snapshot)
                        throw new IllegalArgumentException("Snapshot handles cannot be returned as read results");
                    return value;
                } finally { view.invalidate(); }
            }
        });
    }

    /** 回执观察同样使用独立冷读预算与通知线程；空观察不证明未知提交没有发生。 */
    public ReadHandle<RealmReceiptReader.Observation> readReceipt(RealmReceiptReader reader, RealmIdentity identity,
                                                                 RequestKey key, ReadBudget budget) {
        Objects.requireNonNull(reader, "reader");
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(budget, "budget");
        // 请求键已由领域限定为 ASCII，不能用调用者自报计量省略它的实际长度。
        long charge = Math.addExact(TASK_METADATA_BYTES, (long) key.scope().length() + key.requestId().length());
        return submit(charge, () -> Objects.requireNonNull(reader.observe(identity, key, budget), "receipt observation"));
    }

    /** 有界待办发现走同一冷读通道；只返回候选，不能把扫描线程当作玩家执行资格。 */
    public ReadHandle<List<RealmWorkReader.DueTask>> readDueTasks(RealmWorkReader reader, RealmIdentity identity,
            long dueThrough, RealmWorkReader.DueCursor after, int limit, ReadBudget budget) {
        Objects.requireNonNull(reader); Objects.requireNonNull(identity); Objects.requireNonNull(budget);
        if (dueThrough < 0 || limit <= 0) throw new IllegalArgumentException("Invalid due scan bounds");
        long charge = TASK_METADATA_BYTES + Long.BYTES + Integer.BYTES
                + (after == null ? 0 : Long.BYTES + after.taskId().length());
        return submit(charge, () -> {
            var page = List.copyOf(reader.dueTasks(identity, dueThrough, after, limit, budget));
            if (page.size() > limit) throw new IllegalStateException("Work reader exceeded its page limit");
            return page;
        });
    }

    /** 战斗页同样受连接、结果通知和逐页读取预算约束；非空短页仍须继续翻页。 */
    public ReadHandle<List<RealmWorkReader.PendingBattle>> readPendingBattles(RealmWorkReader reader, RealmIdentity identity,
            String afterId, int limit, ReadBudget budget) {
        Objects.requireNonNull(reader); Objects.requireNonNull(identity); Objects.requireNonNull(budget);
        if (limit <= 0) throw new IllegalArgumentException("Battle page limit must be positive");
        if (afterId != null) new ObjectKey.Battle(afterId); // 计量依据为受限 ASCII 身份，禁止任意捕获文本。
        long charge = TASK_METADATA_BYTES + Integer.BYTES + (afterId == null ? 0 : afterId.length());
        return submit(charge, () -> {
            var page = List.copyOf(reader.pendingBattles(identity, afterId, limit, budget));
            if (page.size() > limit) throw new IllegalStateException("Work reader exceeded its page limit");
            return page;
        });
    }

    private <T> ReadHandle<T> submit(long charge, ReadAction<T> action) {
        synchronized (monitor) {
            if (!accepting) throw new ReadRejectedException(ReadRejectedException.Reason.STOPPED);
            if (outstanding >= limits.maxOutstanding()
                    || charge > limits.maxMetadataBytes() - metadataBytes)
                throw new ReadRejectedException(ReadRejectedException.Reason.CAPACITY);
            var task = new Task<>(charge, action);
            outstanding++;
            metadataBytes += charge;
            work.addLast(task);
            monitor.notifyAll();
            return task;
        }
    }

    /**
     * 非阻塞停止准入并取消队内未开始项；已开始读取不被 interrupt。只在真实读取与全部
     * 结果通知完成后，由通知线程完成隔离 stage；数据库长期不返回时保持未完成。
     */
    public CompletionStage<Void> drain() {
        synchronized (monitor) {
            accepting = false;
            for (Task<?> task : work) task.cancelled = true;
            monitor.notifyAll();
        }
        return drained.minimalCompletionStage();
    }

    /** 有界等待只是观察；超时返回 false，后台继续收敛，不关闭正在使用的快照或释放预算。 */
    public boolean awaitDrain(Duration timeout) throws InterruptedException {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) throw new IllegalArgumentException("Drain wait must not be negative");
        if (threads.contains(Thread.currentThread()))
            throw new IllegalStateException("Cannot wait for cold-read drain inside its worker or notification callback");
        drain();
        try {
            drained.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
            return true;
        } catch (TimeoutException timeoutReached) {
            return false;
        } catch (ExecutionException failure) {
            throw new IllegalStateException("Cold-read drain failed", failure.getCause());
        }
    }

    @Override public void close() {
        try {
            if (!awaitDrain(limits.closeTimeout()))
                throw new IllegalStateException("Cold-read drain timed out; running reads or notifications remain active");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Cold-read drain interrupted; running reads remain active", interrupted);
        }
    }

    public Statistics statistics() {
        synchronized (monitor) {
            int notifying = (int) notifiers.stream().filter(notifier -> notifier.assigned != null).count();
            return new Statistics(accepting, outstanding, metadataBytes, work.size(), executing,
                    notifications.size(), notifying);
        }
    }

    /** 只读装配约束；跨端口用例可验证自己需要的最小并行读取预算，不改变运行中容量。 */
    public ReadExecutionLimits limits() { return limits; }

    public record Statistics(boolean accepting, int outstanding, long metadataBytes, int queued,
                              int executing, int queuedNotifications, int notifying) { }

    private void workLoop() {
        while (true) {
            Task<?> task;
            synchronized (monitor) {
                while (work.isEmpty() && accepting) waitForChange();
                if (work.isEmpty()) return;
                task = work.removeFirst();
                task.started = true;
                executing++;
            }
            task.read();
            synchronized (monitor) {
                executing--;
                // 快照已经关闭。准入已预留完整通知容量，这里只交接，不等慢回调，也不内联执行。
                dispatch(task);
            }
        }
    }

    private void dispatch(Task<?> task) {
        for (Notifier notifier : notifiers) {
            if (notifier.assigned == null) {
                notifier.assigned = task;
                monitor.notifyAll();
                return;
            }
        }
        if (notifications.size() >= limits.notificationQueueCapacity())
            throw new IllegalStateException("Reserved cold-read notification capacity was lost");
        notifications.addLast(task);
        monitor.notifyAll();
    }

    private void notifyLoop(Notifier notifier) {
        while (true) {
            Task<?> task;
            boolean completeDrain = false;
            synchronized (monitor) {
                while (notifier.assigned == null && (accepting || outstanding != 0)) waitForChange();
                task = notifier.assigned;
                if (task == null) {
                    if (!drainCompletionClaimed) { drainCompletionClaimed = true; completeDrain = true; }
                }
            }
            if (task == null) {
                if (completeDrain) drained.complete(null);
                return;
            }
            try {
                task.deliver();
            } finally {
                synchronized (monitor) {
                    outstanding--;
                    metadataBytes -= task.charge;
                    notifier.assigned = notifications.pollFirst();
                    monitor.notifyAll();
                }
            }
        }
    }

    private static Thread thread(String name, Runnable body) {
        var thread = new Thread(body, name);
        thread.setDaemon(true);
        return thread;
    }

    /** 内部线程没有外部取消入口；意外 interrupt 不被误解释为成功排空或已取消在途读取。 */
    private void waitForChange() {
        try { monitor.wait(); } catch (InterruptedException ignored) { }
    }

    private final class Task<T> implements ReadHandle<T> {
        private final long charge;
        private ReadAction<T> action;
        private final CompletableFuture<T> result = new CompletableFuture<>();
        private boolean started;
        private boolean cancelled;
        private T value;
        private Throwable failure;

        private Task(long charge, ReadAction<T> action) { this.charge = charge; this.action = action; }

        @Override public CompletionStage<T> result() { return result.minimalCompletionStage(); }

        @Override public boolean cancelBeforeStart() {
            synchronized (monitor) {
                if (started || cancelled) return false;
                cancelled = true;
                monitor.notifyAll();
                return true;
            }
        }

        private void read() {
            try {
                if (cancelled) throw new CancellationException("Cold read cancelled before snapshot open");
                value = action.read();
            } catch (Throwable error) {
                value = null;
                failure = error;
            } finally {
                // 已完成句柄不再通过 query/reader 继续钉住输入闭包、适配器或连接池。
                action = null;
            }
        }

        private void deliver() {
            try {
                if (failure == null) result.complete(value);
                else result.completeExceptionally(failure);
            } finally {
                value = null; failure = null;
            }
        }
    }

    private static final class Notifier { private Task<?> assigned; }
    @FunctionalInterface private interface ReadAction<T> { T read() throws Exception; }

    private static final class GuardedView implements SnapshotView {
        private final Thread owner = Thread.currentThread();
        private RealmStateReader.Snapshot delegate;
        private GuardedView(RealmStateReader.Snapshot delegate) { this.delegate = Objects.requireNonNull(delegate); }
        private RealmStateReader.Snapshot checked() {
            if (Thread.currentThread() != owner || delegate == null)
                throw new IllegalStateException("Snapshot view is only valid inside its owning read query");
            return delegate;
        }
        private void invalidate() { delegate = null; }
        @Override public RealmIdentity identity() { return checked().identity(); }
        @Override public long highSequence() { return checked().highSequence(); }
        @Override public Optional<VersionedState> find(ObjectKey key) throws StorageAccessException { return checked().find(key); }
        @Override public Optional<VersionedState> findPlayerByAccount(long id) throws StorageAccessException {
            return checked().findPlayerByAccount(id);
        }
        @Override public void scan(ObjectKind kind, Consumer<RealmStateReader.StateEntry> consumer) throws StorageAccessException {
            checked().scan(kind, consumer);
        }
        @Override public void scanOwned(ObjectKind kind, String playerId, Consumer<RealmStateReader.StateEntry> consumer)
                throws StorageAccessException { checked().scanOwned(kind, playerId, consumer); }
        @Override public void scanActiveMarches(String playerId, Consumer<RealmStateReader.StateEntry> consumer)
                throws StorageAccessException { checked().scanActiveMarches(playerId, consumer); }
        @Override public void scanMovingMarches(Consumer<RealmStateReader.StateEntry> consumer)
                throws StorageAccessException { checked().scanMovingMarches(consumer); }
    }
}
