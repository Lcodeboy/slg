package com.slg.game.application.commit;

import com.slg.game.application.port.CommitPayloadSizer;
import com.slg.game.application.port.RealmOwnership;
import com.slg.game.application.port.RealmTransaction;
import com.slg.game.domain.persistence.RequestKey;
import com.slg.game.domain.persistence.WriterToken;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.slg.game.application.commit.CommitResult.Reason.*;

/**
 * 单区的有界异步提交器。队列只保存完整不可变 CommitRequest；玩法读取/计算之前的对象
 * 资格及提交后的发布资格由应用执行层持有。本类额外保持同 RequestKey 的 FIFO，不能
 * 用数据库工作线程等待另一个请求的资格，也不能把独立业务拼成一个事务。
 *
 * <p>gate 只保护短暂内存状态转换；SQL、所有权 I/O 和外部 Future 回调都在锁外运行。
 * 数量/字节额度直到结果通知返回才释放，避免把 JDBC 完成后堆积的回调变成无界第二队列。
 * UNKNOWN 冻结整区准入，旧任期全部在途尝试返回后才隔离并核对；从不自动重投 commit。</p>
 */
public final class BoundedRealmCommitService implements CommitCoordinator {
    private final Object gate = new Object();
    private final RealmTransaction transaction;
    private final RealmOwnership ownership;
    private final CommitPayloadSizer sizer;
    private final CommitQueueLimits limits;
    private final ThreadPoolExecutor database;
    private final ThreadPoolExecutor notifications;
    private final ThreadPoolExecutor control;
    private final ScheduledExecutorService timer;
    private final EnumMap<CommitRequest.Origin, ArrayDeque<Task>> queues = new EnumMap<>(CommitRequest.Origin.class);
    private final Map<RequestKey, ArrayDeque<Task>> requests = new HashMap<>();
    private final Set<Task> retained = new LinkedHashSet<>();
    private final Set<Task> unknown = new LinkedHashSet<>();
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private WriterToken writer;
    private WriterToken recoveryBase;
    private WriterToken replacement;
    private boolean isolated;
    private boolean fenceAttempted;
    private boolean ownerLost;
    private boolean stopping;
    private boolean closed;
    private boolean finishing;
    private boolean controlBusy;
    private int executing;
    private int clientTasks;
    private long retainedBytes;
    private long clientBytes;
    private int publicationHolds;
    private int nextOrigin;
    private long lastRenewed = System.nanoTime();

    public BoundedRealmCommitService(WriterToken initialWriter, RealmTransaction transaction,
                                     RealmOwnership ownership, CommitPayloadSizer sizer, CommitQueueLimits limits) {
        this.writer = Objects.requireNonNull(initialWriter, "initialWriter");
        this.transaction = Objects.requireNonNull(transaction, "transaction");
        this.ownership = Objects.requireNonNull(ownership, "ownership");
        this.sizer = Objects.requireNonNull(sizer, "sizer");
        this.limits = Objects.requireNonNull(limits, "limits");
        for (var origin : CommitRequest.Origin.values()) queues.put(origin, new ArrayDeque<>());
        database = pool(limits.databaseWorkers(), limits.databaseWorkers(), "commit-jdbc");
        // 每个已准入任务至多一个通知，加一个 drain 通知；容量在准入时已经保留。
        notifications = pool(limits.notificationWorkers(), Math.addExact(limits.maxTasks(), 1), "commit-notify");
        control = pool(1, 1, "commit-owner");
        timer = Executors.newSingleThreadScheduledExecutor(threads("commit-timer"));
        timer.scheduleWithFixedDelay(this::tick, limits.controlInterval().toNanos(),
                limits.controlInterval().toNanos(), TimeUnit.NANOSECONDS);
    }

    @Override public CommitHandle submit(CommitRequest request) {
        Objects.requireNonNull(request, "request");
        long received = System.nanoTime();
        synchronized (gate) {
            var rejection = admissionState(request);
            if (rejection != null) return rejected(request, rejection);
        }
        final long bytes;
        try {
            bytes = sizer.measure(request);
            if (bytes <= 0) return rejected(request, INVALID_DECISION);
        } catch (RuntimeException invalid) { return rejected(request, INVALID_DECISION); }
        synchronized (gate) {
            // 编码发生在锁外；期间可能出现 UNKNOWN、换任期或关闭，必须重新核验准入。
            var rejection = admissionState(request);
            if (rejection != null) return rejected(request, rejection);
            boolean client = request.origin() == CommitRequest.Origin.CLIENT;
            if (retained.size() >= limits.maxTasks() || bytes > limits.maxBytes() - retainedBytes
                    || client && (clientTasks >= limits.maxTasks() - limits.systemReservedTasks()
                    || bytes > limits.maxBytes() - limits.systemReservedBytes() - clientBytes))
                return rejected(request, QUEUE_FULL);
            boolean immediate = request.queueBudget().isZero();
            if (immediate && (executing >= limits.databaseWorkers() || requests.containsKey(request.decision().request())))
                return rejected(request, QUEUE_TIMEOUT);
            long wait = Math.min(nanos(request.queueBudget()), limits.maxQueueWait().toNanos());
            if (!immediate && System.nanoTime() - received >= wait) return rejected(request, QUEUE_TIMEOUT);
            var task = new Task(request, bytes, received, wait);
            retained.add(task); retainedBytes += bytes;
            if (client) { clientTasks++; clientBytes += bytes; }
            queues.get(task.origin).addLast(task);
            requests.computeIfAbsent(task.key, ignored -> new ArrayDeque<>()).addLast(task);
            pumpLocked();
            return task;
        }
    }

    @Override public Optional<WriterToken> currentWriter() {
        synchronized (gate) { return canStart() ? Optional.of(writer) : Optional.empty(); }
    }

    @Override public PublicationBarrier isolatePublication() {
        synchronized (gate) {
            if (closed || finishing) throw new IllegalStateException("Commit coordinator is closed or drained");
            publicationHolds = Math.addExact(publicationHolds, 1);
            cancelQueuedLocked(ISOLATED);
        }
        var released = new AtomicBoolean();
        return () -> {
            if (!released.compareAndSet(false, true)) return;
            synchronized (gate) {
                publicationHolds--;
                pumpLocked();
                finishIfDrainedLocked();
            }
        };
    }

    @Override public CompletionStage<Void> drain() {
        synchronized (gate) {
            stopping = true;
            cancelQueuedLocked(STOPPING);
            finishIfDrainedLocked();
            return drained.minimalCompletionStage();
        }
    }

    public CommitQueueSnapshot snapshot() {
        synchronized (gate) {
            var mode = closed ? CommitQueueSnapshot.Mode.CLOSED : ownerLost ? CommitQueueSnapshot.Mode.OWNER_LOST
                    : stopping ? CommitQueueSnapshot.Mode.DRAINING
                    : isolated || publicationHolds > 0 ? CommitQueueSnapshot.Mode.ISOLATED : CommitQueueSnapshot.Mode.ACTIVE;
            int queued = queues.values().stream().mapToInt(ArrayDeque::size).sum();
            return new CommitQueueSnapshot(mode, writer, retained.size(), retainedBytes,
                    queued, executing, unknown.size(), publicationHolds);
        }
    }

    private CommitResult.Reason admissionState(CommitRequest request) {
        if (stopping || closed) return STOPPING;
        if (ownerLost || !writer.equals(request.writer())) return OWNER_LOST;
        if (isolated || publicationHolds > 0) return ISOLATED;
        return null;
    }
    private boolean canStart() { return !stopping && !closed && !ownerLost && !isolated && publicationHolds == 0; }

    /** 四来源轮转；同请求只允许头项启动，阻塞请求不会挡住该来源中其它独立请求。 */
    private void pumpLocked() {
        if (!canStart()) return;
        expireLocked();
        var origins = CommitRequest.Origin.values();
        while (executing < limits.databaseWorkers()) {
            Task selected = null;
            for (int offset = 0; offset < origins.length && selected == null; offset++) {
                int index = (nextOrigin + offset) % origins.length;
                for (var candidate : queues.get(origins[index])) {
                    if (requests.get(candidate.key).peekFirst() == candidate) {
                        selected = candidate; nextOrigin = (index + 1) % origins.length; break;
                    }
                }
            }
            if (selected == null) return;
            queues.get(selected.origin).remove(selected);
            selected.phase = CommitPhase.EXECUTING;
            executing++;
            Task task = selected;
            try { database.execute(() -> execute(task)); }
            catch (RejectedExecutionException stopped) {
                // 从未交给存储端口，能够确定拒绝；不是把已开始的数据库任务取消掉。
                executing--;
                settleLocked(task, new CommitResult.NotCommitted(STOPPING));
            }
        }
    }

    private void execute(Task task) {
        RealmTransaction.CommitAttempt attempt;
        try {
            attempt = Objects.requireNonNull(transaction.commit(task.request.writer(), task.request.decision()));
            if (attempt instanceof RealmTransaction.CommitAttempt.Settled settled) verifyResult(task, settled.result());
        } catch (Throwable failure) {
            // 端口异常本身不证明是否提交。包括未知内部故障在内，均保持资格并走隔离核对。
            attempt = new RealmTransaction.CommitAttempt.Uncertain();
        }
        synchronized (gate) {
            executing--;
            if (attempt instanceof RealmTransaction.CommitAttempt.Settled settled) {
                if (settled.result() instanceof CommitResult.NotCommitted rejected && rejected.reason() == OWNER_LOST)
                    isolateLocked();
                settleLocked(task, settled.result());
            } else {
                task.phase = CommitPhase.RECONCILING;
                unknown.add(task);
                isolateLocked();
            }
            pumpLocked();
            finishIfDrainedLocked();
        }
    }

    private void isolateLocked() {
        if (!isolated) {
            isolated = true;
            recoveryBase = writer;
            replacement = null;
            fenceAttempted = false;
        }
        cancelQueuedLocked(OWNER_LOST);
    }

    private void expireLocked() {
        long now = System.nanoTime();
        for (var task : new ArrayList<>(retained))
            if (task.phase == CommitPhase.QUEUED && task.waitNanos > 0 && now - task.received >= task.waitNanos)
                settleLocked(task, new CommitResult.NotCommitted(QUEUE_TIMEOUT));
    }
    private void cancelQueuedLocked(CommitResult.Reason reason) {
        for (var task : new ArrayList<>(retained))
            if (task.phase == CommitPhase.QUEUED) settleLocked(task, new CommitResult.NotCommitted(reason));
    }

    private void settleLocked(Task task, CommitResult result) {
        queues.get(task.origin).remove(task);
        unknown.remove(task);
        task.phase = CommitPhase.SETTLED;
        // 唯一完成点：JDBC 槽先释放，通知使用已预留容量；不在 gate 或 JDBC 线程执行调用方回调。
        notifications.execute(() -> {
            try { task.future.complete(result); }
            finally {
                synchronized (gate) {
                    if (!retained.remove(task)) throw new IllegalStateException("Commit capacity released twice");
                    retainedBytes -= task.bytes;
                    if (task.origin == CommitRequest.Origin.CLIENT) { clientTasks--; clientBytes -= task.bytes; }
                    var ordered = requests.get(task.key);
                    ordered.remove(task);
                    if (ordered.isEmpty()) requests.remove(task.key);
                    task.request = null;
                    pumpLocked();
                    finishIfDrainedLocked();
                }
            }
        });
    }

    private void tick() {
        synchronized (gate) {
            if (closed || ownerLost) return;
            expireLocked();
            pumpLocked();
            if (controlBusy || finishing) return;
            // 先吸收全部旧任期的本地在途结果，避免 fence 后遗漏迟到 UNKNOWN；真正数据库中的
            // 失联旧事务仍由 owner 排他锁等待。恢复/续期共用单一控制序列，不能并行互相覆盖。
            boolean recovery = isolated && executing == 0;
            boolean renew = !isolated && !stopping && System.nanoTime() - lastRenewed >= limits.renewInterval().toNanos();
            if (!recovery && !renew) { finishIfDrainedLocked(); return; }
            controlBusy = true;
            try { control.execute(() -> controlStep(recovery)); }
            catch (RejectedExecutionException stopped) { controlBusy = false; }
        }
    }

    private void controlStep(boolean recovery) {
        try {
            if (recovery) recover();
            else {
                WriterToken token;
                synchronized (gate) { token = writer; }
                boolean renewed;
                try { renewed = ownership.renew(token); }
                catch (Exception unavailable) { renewed = false; }
                synchronized (gate) {
                    // 旧续期调用可能晚于业务 UNKNOWN 返回；成功也不能取消已经建立的隔离屏障。
                    if (!isolated && writer.equals(token)) {
                        if (renewed) lastRenewed = System.nanoTime();
                        else isolateLocked();
                    }
                }
            }
        } catch (Throwable failure) {
            // 定时线程不会因一次控制失败消失；保守保持隔离，下一次只核对，不重投业务。
            synchronized (gate) { if (!ownerLost) isolateLocked(); }
        } finally {
            synchronized (gate) { controlBusy = false; finishIfDrainedLocked(); }
        }
    }

    private void recover() throws Exception {
        WriterToken base;
        WriterToken candidate;
        boolean fence;
        synchronized (gate) {
            base = recoveryBase;
            candidate = replacement;
            fence = candidate == null && !fenceAttempted;
            if (fence) fenceAttempted = true;
        }
        if (candidate == null) {
            if (fence) {
                // 此调用失败可能已改变数据库 epoch；下一轮绝不再盲目 CAS 同一旧 token。
                candidate = ownership.fence(base);
            } else {
                var observed = ownership.observe(base.realm());
                if (!observed.realm().equals(base.realm()) || observed.epoch() < base.epoch()) return;
                // 合法的本实例租约到期仍保留 active 和 instance。inactive 已抹去身份，可能是
                // 其他实例接管后又释放，不能把“当前无人占有”猜成仍获准由本实例自动恢复。
                if (!observed.active() || observed.instanceId() == null
                        || !observed.instanceId().equals(base.instanceId())) {
                    synchronized (gate) { loseOwnershipLocked(); }
                    return;
                }
                if (observed.active() && observed.leaseValid()) return;
                // fence 确认丢失后停止续期，等待数据库确认失效。恢复端口在排他锁内再次比较
                // 这份观察的完整身份；期间别人接管又释放也必须拒绝，不能退回普通 acquire。
                candidate = ownership.reacquire(observed);
            }
            if (!candidate.realm().equals(base.realm()) || !candidate.instanceId().equals(base.instanceId())
                    || candidate.epoch() <= base.epoch()) throw new IllegalStateException("Invalid ownership replacement");
            synchronized (gate) { replacement = candidate; lastRenewed = System.nanoTime(); }
        } else {
            // 已确认的 replacement 在分批核对期间可续期；若续期失联/失效，再次停止续期并观察。
            boolean renewalDue;
            synchronized (gate) { renewalDue = System.nanoTime() - lastRenewed >= limits.renewInterval().toNanos(); }
            if (renewalDue) {
                boolean renewed;
                try { renewed = ownership.renew(candidate); }
                catch (Exception unavailable) { renewed = false; }
                if (!renewed) {
                    synchronized (gate) { recoveryBase = candidate; replacement = null; fenceAttempted = true; }
                    return;
                }
                synchronized (gate) { lastRenewed = System.nanoTime(); }
            }
        }
        List<Task> batch;
        synchronized (gate) { batch = unknown.stream().limit(limits.databaseWorkers()).toList(); }
        for (var task : batch) {
            RealmTransaction.CommitAttempt attempt;
            try {
                attempt = transaction.resolveAfterFence(task.request.writer(), candidate, task.key, task.request.decision().digest());
                if (attempt instanceof RealmTransaction.CommitAttempt.Settled settled) verifyResult(task, settled.result());
            } catch (Exception unavailable) { continue; }
            if (attempt instanceof RealmTransaction.CommitAttempt.Settled settled) {
                CommitResult result = settled.result();
                // 核对结果永远不是本次新应用的草案，应用层必须重载/核对缓存。
                if (result instanceof CommitResult.Committed committed) result = new CommitResult.Committed(committed.receipt(), false);
                synchronized (gate) { settleLocked(task, result); }
            }
        }
        synchronized (gate) {
            if (unknown.isEmpty() && executing == 0) {
                writer = candidate;
                replacement = null; recoveryBase = null; fenceAttempted = false; isolated = false;
                lastRenewed = System.nanoTime();
                pumpLocked();
            }
        }
    }

    private void loseOwnershipLocked() {
        ownerLost = true;
        cancelQueuedLocked(OWNER_LOST);
        // 被其它实例接管后永久停止本实例自动夺回。UNKNOWN 及 drain 仍未完成，halt 不等于排空。
        // 此处 executing 已为0；既有通知继续交付，通知线程空闲后自行退出，不保留轮询后台线程。
        timer.shutdown(); database.shutdown(); control.shutdown();
    }

    private void finishIfDrainedLocked() {
        if (!stopping || finishing || closed || !retained.isEmpty() || executing != 0 || controlBusy
                || !unknown.isEmpty() || publicationHolds != 0) return;
        finishing = true;
        timer.shutdown(); database.shutdown(); control.shutdown();
        notifications.execute(() -> {
            synchronized (gate) { closed = true; }
            try { drained.complete(null); }
            finally { notifications.shutdown(); }
        });
    }

    private static void verifyResult(Task task, CommitResult result) {
        if (result instanceof CommitResult.Committed committed
                && (!committed.receipt().request().equals(task.key)
                || !committed.receipt().digest().equals(task.request.decision().digest())))
            throw new IllegalStateException("Transaction result belongs to another decision");
    }

    private CommitHandle rejected(CommitRequest request, CommitResult.Reason reason) {
        var task = new Task(request, 0, 0, 0);
        task.phase = CommitPhase.SETTLED;
        task.future.complete(new CommitResult.NotCommitted(reason));
        task.request = null;
        return task;
    }

    private final class Task implements CommitHandle {
        private CommitRequest request;
        private final RequestKey key;
        private final CommitRequest.Origin origin;
        private final long bytes;
        private final long received;
        private final long waitNanos;
        private final CompletableFuture<CommitResult> future = new CompletableFuture<>();
        private volatile CommitPhase phase = CommitPhase.QUEUED;
        Task(CommitRequest request, long bytes, long received, long waitNanos) {
            this.request = request; key = request.decision().request(); origin = request.origin();
            this.bytes = bytes; this.received = received; this.waitNanos = waitNanos;
        }
        @Override public RequestKey requestKey() { return key; }
        @Override public CommitPhase phase() { return phase; }
        @Override public CompletionStage<CommitResult> settlement() { return future.minimalCompletionStage(); }
        @Override public boolean cancelBeforeStart() {
            synchronized (gate) {
                if (phase != CommitPhase.QUEUED) return false;
                settleLocked(this, new CommitResult.NotCommitted(CANCELLED));
                pumpLocked();
                return true;
            }
        }
    }

    private static long nanos(Duration duration) {
        try { return duration.toNanos(); } catch (ArithmeticException overflow) { return Long.MAX_VALUE; }
    }
    private static ThreadPoolExecutor pool(int workers, int capacity, String name) {
        var pool = new ThreadPoolExecutor(workers, workers, 1, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(capacity), threads(name), new ThreadPoolExecutor.AbortPolicy());
        pool.allowCoreThreadTimeOut(true);
        return pool;
    }
    private static ThreadFactory threads(String prefix) {
        var count = new AtomicInteger();
        return action -> {
            var thread = new Thread(action, prefix + "-" + count.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
