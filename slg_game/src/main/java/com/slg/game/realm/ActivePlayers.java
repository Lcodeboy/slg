package com.slg.game.realm;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * 新区玩家的驻留目录及单玩家命令入口。每个繁忙账号只有一个加载或执行通行证。
 * 目录锁只维护队列和执行资格，读取 RocksDB、执行业务和完成外部 future 都在锁外。
 */
public final class ActivePlayers implements AutoCloseable {
    @FunctionalInterface public interface Loader { ActivePlayer load(String account) throws Exception; }
    @FunctionalInterface public interface Work<T> { T run(ActivePlayer player) throws Exception; }

    /** 加载失败发生在进入业务之前；这批等待命令均未执行。 */
    public static final class LoadFailed extends IOException {
        LoadFailed(Throwable cause) { super("Player activation failed before execution", cause); }
    }

    public static final class TargetLoadFailed extends IOException {
        TargetLoadFailed(Throwable cause) { super("March target lookup failed before execution", cause); }
    }

    private enum Phase { IDLE, LOADING, RESOLVING, WAITING_TARGET, READY, RUNNING }

    private static final class Slot {
        final String account;
        final ArrayDeque<Task<?>> waiting = new ArrayDeque<>();
        ActivePlayer player;
        Phase phase = Phase.IDLE;
        long idleSince;

        Slot(String account) { this.account = account; }
    }

    private static final class Task<T> {
        final Slot slot;
        final long order;
        final Work<T> work;
        final long deadline;
        final CompletableFuture<T> result = new CompletableFuture<>();
        Work<String> resolveTarget;
        String target;
        ScheduledFuture<?> expiry;

        Task(Slot slot, long order, Work<T> work, long deadline, Work<String> resolveTarget) {
            this.slot = slot;
            this.order = order;
            this.work = work;
            this.deadline = deadline;
            this.resolveTarget = resolveTarget;
        }
    }

    /** 等待名额和正在执行者一起计入上限。取得玩家与目标两者的执行资格后才能从队列转为 running。 */
    private static final class TargetQueue {
        final TreeMap<Long, Task<?>> queue = new TreeMap<>();
        Task<?> running;
    }

    private final Object monitor = new Object();
    private final Map<String, Slot> slots = new HashMap<>();
    private final Map<String, TargetQueue> targets = new HashMap<>();
    // 只登记完全空闲的账号，按进入空闲的时间排序。容量回收与过期检查只需查看队首。
    private final LinkedHashSet<Slot> idle = new LinkedHashSet<>();
    private final Loader loader;
    private final ExecutorService workers, readers;
    private final ScheduledThreadPoolExecutor timer;
    private final int capacity, perPlayerCapacity, maxResidents;
    private final long idleNanos;
    private final LongSupplier nanoTime;
    private boolean closed;
    private int outstanding, loading, running, residents;
    private long loads, loadFailures, evictions, rejected, expired, lastOrder, targetRejected;

    public ActivePlayers(Loader loader, int workers, int loadWorkers, int capacity,
                         int perPlayerCapacity, int maxResidents, Duration idleTtl) {
        this(loader, workers, loadWorkers, capacity, perPlayerCapacity, maxResidents, idleTtl, System::nanoTime);
    }

    ActivePlayers(Loader loader, int workers, int loadWorkers, int capacity, int perPlayerCapacity,
                  int maxResidents, Duration idleTtl, LongSupplier nanoTime) {
        this.loader = Objects.requireNonNull(loader);
        this.nanoTime = Objects.requireNonNull(nanoTime);
        this.idleNanos = Objects.requireNonNull(idleTtl).toNanos();
        if (workers <= 0 || loadWorkers <= 0 || capacity <= 0 || perPlayerCapacity <= 0
                || perPlayerCapacity > capacity || maxResidents <= 0 || idleNanos <= 0) {
            throw new IllegalArgumentException("Invalid active player limits");
        }
        this.capacity = capacity;
        this.perPlayerCapacity = perPlayerCapacity;
        this.maxResidents = maxResidents;
        this.workers = pool("realm-player-", workers, capacity);
        this.readers = pool("realm-player-load-", loadWorkers, capacity);
        this.timer = new ScheduledThreadPoolExecutor(1, Thread.ofPlatform().daemon().name("realm-player-timer").factory());
        timer.setRemoveOnCancelPolicy(true);
        timer.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        long interval = Math.clamp(idleNanos, TimeUnit.MILLISECONDS.toNanos(10), TimeUnit.SECONDS.toNanos(30));
        timer.scheduleWithFixedDelay(this::evictIdle, interval, interval, TimeUnit.NANOSECONDS);
    }

    private static ExecutorService pool(String prefix, int threads, int queueCapacity) {
        return new ThreadPoolExecutor(threads, threads, 0, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(queueCapacity), Thread.ofPlatform().daemon().name(prefix, 0).factory());
    }

    /**
     * 受理在返回前完成。deadline 属于 System.nanoTime 时间域，只限制业务开始时间；
     * 已经开始的任务不会因为客户端超时而释放同玩家的执行资格。
     */
    public <T> CompletableFuture<T> submit(String account, long deadlineNanos, Work<T> work) {
        return submit(account, deadlineNanos, null, null, work);
    }

    /** 到期扫描已按 (dueAt, taskId) 确定投递次序，立即登记采集点；玩家冷加载先后不能颠倒两个已登记任务。 */
    public <T> CompletableFuture<T> submitForTarget(String account, long deadlineNanos, String target, Work<T> work) {
        return submit(account, deadlineNanos, target, null, work);
    }

    /** 召回仅携带行军 ID。轮到本玩家后在读取线程解析目标，支持同一邮箱里刚刚创建的行军。 */
    public <T> CompletableFuture<T> submitResolvingTarget(String account, long deadlineNanos,
                                                         Work<String> resolveTarget, Work<T> work) {
        return submit(account, deadlineNanos, null, Objects.requireNonNull(resolveTarget), work);
    }

    private <T> CompletableFuture<T> submit(String account, long deadlineNanos, String target,
                                            Work<String> resolveTarget, Work<T> work) {
        Objects.requireNonNull(account); Objects.requireNonNull(work);
        synchronized (monitor) {
            long now = nanoTime.getAsLong();
            if (deadlineNanos - now <= 0) {
                expired++;
                return CompletableFuture.failedFuture(new QueueDeadlineException());
            }
            Slot existing = slots.get(account);
            // 请求路径最多回收一个空闲项；大批同时过期由维护线程处理。
            if (existing != null && existing.phase == Phase.IDLE && now - existing.idleSince >= idleNanos) {
                evict(existing);
                existing = null;
            }
            if (closed || outstanding >= capacity || (existing != null && existing.waiting.size()
                    + (existing.phase == Phase.RUNNING ? 1 : 0) >= perPlayerCapacity)) return refuse();
            if (targetFull(target)) return refuse();
            // 即使请求在读取过程中全部取消，仍要把该读取计入繁忙账号上限。
            boolean activating = existing == null || existing.phase == Phase.IDLE;
            if (activating && slots.size() - idle.size() >= capacity) return refuse();
            if (existing == null && slots.size() >= maxResidents) {
                if (idle.isEmpty()) return refuse();
                evict(idle.getFirst());
            }
            Slot slot = existing != null ? existing : new Slot(account);
            if (existing == null) slots.put(account, slot);
            idle.remove(slot);
            Task<T> task = new Task<>(slot, ++lastOrder, work, deadlineNanos, resolveTarget);
            register(task, target);
            slot.waiting.addLast(task);
            outstanding++;
            task.expiry = timer.schedule(() -> expire(slot, task), deadlineNanos - now, TimeUnit.NANOSECONDS);
            task.result.whenComplete((ignored, error) -> {
                if (task.result.isCancelled()) cancelQueued(slot, task);
            });
            if (slot.phase == Phase.IDLE) dispatch(slot);
            return task.result;
        }
    }

    private boolean targetFull(String target) {
        TargetQueue lane = targets.get(target);
        if (lane == null || lane.queue.size() < perPlayerCapacity) return false;
        targetRejected++;
        return true;
    }

    private void register(Task<?> task, String target) {
        if (target == null) return;
        task.target = target;
        // 未解析的较早召回可能晚于同玩家的下一条到达任务登记。用最初的受理次序排序，
        // 否则目标队首等玩家队首、玩家队首又等目标队首，就会互相阻塞。
        targets.computeIfAbsent(target, ignored -> new TargetQueue()).queue.put(task.order, task);
    }

    private boolean canStart(Task<?> task) {
        if (task.target == null) return true;
        TargetQueue lane = targets.get(task.target);
        return lane.running == null && lane.queue.firstEntry().getValue() == task;
    }

    private void releaseTarget(Task<?> task) {
        if (task.target == null) return;
        TargetQueue lane = targets.get(task.target);
        lane.queue.remove(task.order);
        if (lane.running == task) lane.running = null;
        if (lane.queue.isEmpty()) {
            targets.remove(task.target);
        } else if (lane.running == null) {
            Slot next = lane.queue.firstEntry().getValue().slot;
            if (next.phase == Phase.WAITING_TARGET && next != task.slot) dispatch(next);
        }
    }

    private <T> CompletableFuture<T> refuse() {
        rejected++;
        return CompletableFuture.failedFuture(new RejectedExecutionException("Player capacity is full or stopping"));
    }

    /** 仅在本账号上一项加载/工作已结束且持 monitor 时调用。每轮只把一个账号交给池一次。 */
    private void dispatch(Slot slot) {
        if (closed) return;
        if (slot.player != null && !slot.player.valid()) discard(slot);
        if (slot.waiting.isEmpty()) {
            slot.phase = Phase.IDLE;
            if (slot.player == null) {
                slots.remove(slot.account);
            } else {
                slot.idleSince = nanoTime.getAsLong();
                idle.add(slot);
            }
        } else if (slot.player == null) {
            slot.phase = Phase.LOADING;
            loading++;
            readers.execute(() -> load(slot));
        } else {
            Task<?> head = slot.waiting.getFirst();
            if (head.resolveTarget != null) {
                slot.phase = Phase.RESOLVING;
                readers.execute(() -> resolve(head));
            } else if (canStart(head)) {
                slot.phase = Phase.READY;
                workers.execute(() -> runOne(slot, head));
            } else {
                slot.phase = Phase.WAITING_TARGET;
            }
        }
    }

    /** 查询时只固定当前玩家邮箱顺序，未持目标执行资格或目录锁，不占业务线程。 */
    private void resolve(Task<?> task) {
        String target = null;
        Throwable failure = null;
        try { target = task.resolveTarget.run(task.slot.player); }
        catch (Throwable error) { failure = new TargetLoadFailed(error); }
        synchronized (monitor) {
            Slot slot = task.slot;
            if (slot.waiting.peekFirst() == task) {
                task.resolveTarget = null;
                if (failure == null && targetFull(target)) {
                    rejected++;
                    failure = new RejectedExecutionException("Realm target capacity full");
                }
                if (failure == null) register(task, target);
                else removeWaiting(slot, task);
            }
            dispatch(slot);
        }
        if (failure != null) task.result.completeExceptionally(failure);
    }

    private void load(Slot slot) {
        synchronized (monitor) {
            if (closed || slot.waiting.isEmpty()) {
                loading--;
                dispatch(slot);
                return;
            }
            loads++;
        }
        ActivePlayer loaded;
        try {
            loaded = Objects.requireNonNull(loader.load(slot.account));
            if (!loaded.valid() || !slot.account.equals(loaded.accountId()))
                throw new IOException("Loader returned an invalid player or account");
        } catch (Throwable cause) {
            List<Task<?>> abandoned;
            synchronized (monitor) {
                loadFailures++; loading--;
                abandoned = drain(slot);
                slots.remove(slot.account);
            }
            LoadFailed failure = new LoadFailed(cause);
            for (Task<?> task : abandoned) task.result.completeExceptionally(failure);
            return;
        }
        synchronized (monitor) {
            loading--;
            if (!closed && !slot.waiting.isEmpty()) {
                slot.player = loaded;
                residents++;
            }
            dispatch(slot);
        }
    }

    private void runOne(Slot slot, Task<?> task) {
        boolean timedOut, started;
        synchronized (monitor) {
            // 排入线程池后仍可能取消、超时，或有更早受理的召回刚解析到同一矿点。
            if (slot.waiting.peekFirst() != task || !canStart(task)) { dispatch(slot); return; }
            slot.waiting.removeFirst();
            task.expiry.cancel(false);
            timedOut = task.deadline - nanoTime.getAsLong() <= 0;
            started = !timedOut && !task.result.isCancelled();
            if (!started) {
                if (timedOut) expired++;
                outstanding--;
                releaseTarget(task);
                dispatch(slot);
            } else {
                slot.phase = Phase.RUNNING;
                running++;
                if (task.target != null) targets.get(task.target).running = task;
            }
        }
        if (started) execute(slot, task);
        else if (timedOut) task.result.completeExceptionally(new QueueDeadlineException());
    }

    private <T> void execute(Slot slot, Task<T> task) {
        T result = null;
        Throwable error = null;
        try { result = task.work.run(slot.player); }
        catch (Throwable failure) { error = failure; }
        synchronized (monitor) {
            releaseTarget(task);
            finish(slot, error != null || !slot.player.valid());
        }
        if (error == null) task.result.complete(result);
        else task.result.completeExceptionally(error);
    }

    private void finish(Slot slot, boolean invalidate) {
        running--; outstanding--;
        if (invalidate) discard(slot);
        dispatch(slot);
    }

    private void discard(Slot slot) {
        slot.player.invalidate();
        slot.player = null;
        residents--;
    }

    private void expire(Slot slot, Task<?> task) {
        synchronized (monitor) {
            if (!removeWaiting(slot, task)) return;
            expired++;
            if (slot.phase == Phase.WAITING_TARGET) dispatch(slot);
        }
        task.result.completeExceptionally(new QueueDeadlineException());
    }

    private void cancelQueued(Slot slot, Task<?> task) {
        synchronized (monitor) {
            if (removeWaiting(slot, task) && slot.phase == Phase.WAITING_TARGET) dispatch(slot);
        }
    }

    private boolean removeWaiting(Slot slot, Task<?> task) {
        if (!slot.waiting.remove(task)) return false;
        outstanding--;
        task.expiry.cancel(false);
        releaseTarget(task);
        return true;
    }

    /** 主动维护也可调用；加载中、已经排队或正在提交的对象绝不会出现在 idle 中。 */
    public int evictIdle() {
        synchronized (monitor) { return evictExpired(nanoTime.getAsLong()); }
    }

    private int evictExpired(long now) {
        int count = 0;
        while (!idle.isEmpty()) {
            Slot oldest = idle.getFirst();
            if (now - oldest.idleSince < idleNanos) break;
            evict(oldest);
            count++;
        }
        return count;
    }

    private void evict(Slot slot) {
        idle.remove(slot); slots.remove(slot.account);
        discard(slot);
        evictions++;
    }

    /** 已持 monitor，移出所有尚未开始的任务；调用者在锁外完成 future。 */
    private List<Task<?>> drain(Slot slot) {
        List<Task<?>> queued = new ArrayList<>(slot.waiting);
        slot.waiting.clear();
        outstanding -= queued.size();
        for (Task<?> task : queued) {
            task.expiry.cancel(false);
            releaseTarget(task);
        }
        return queued;
    }

    public Map<String, Object> metrics() {
        synchronized (monitor) {
            return Map.ofEntries(Map.entry("entries", slots.size()), Map.entry("residents", residents),
                    Map.entry("loading", loading), Map.entry("running", running), Map.entry("outstanding", outstanding),
                    Map.entry("loads", loads), Map.entry("loadFailures", loadFailures), Map.entry("evictions", evictions),
                    Map.entry("targets", targets.size()), Map.entry("targetRejected", targetRejected),
                    Map.entry("rejected", rejected), Map.entry("expired", expired), Map.entry("closed", closed));
        }
    }

    /** 由进程生命周期线程调用：先拒绝等待项，再等在途加载和业务操作收尾，之后才可关卷。 */
    @Override public void close() {
        List<Task<?>> abandoned = new ArrayList<>();
        synchronized (monitor) {
            if (!closed) {
                closed = true;
                for (Slot slot : slots.values()) abandoned.addAll(drain(slot));
                timer.shutdown();
                readers.shutdown();
                workers.shutdown();
            }
        }
        for (Task<?> task : abandoned) {
            task.result.completeExceptionally(new RejectedExecutionException("Player runtime stopping"));
        }
        boolean interrupted = false;
        for (ExecutorService pool : List.of(workers, readers, timer)) {
            while (!pool.isTerminated()) {
                try { pool.awaitTermination(1, TimeUnit.DAYS); }
                catch (InterruptedException e) { interrupted = true; }
            }
        }
        synchronized (monitor) {
            for (Slot slot : slots.values()) if (slot.player != null) discard(slot);
            slots.clear();
            idle.clear();
        }
        if (interrupted) Thread.currentThread().interrupt();
    }
}
