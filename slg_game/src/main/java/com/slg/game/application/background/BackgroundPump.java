package com.slg.game.application.background;

import com.slg.core.battle.BattleResult;
import com.slg.game.application.commit.CommitResult;
import com.slg.game.application.port.*;
import com.slg.game.application.read.BoundedReadExecutor;
import com.slg.game.domain.persistence.RealmIdentity;
import com.slg.game.domain.player.PlayerIdentity;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;

/**
 * 权威待办的有界发现与派发协调器。只扫描候选、预留容量、投递用例和处理完成通知；数据库
 * I/O 在独立冷读池，资产变化只经统一执行器。Worker 期间不持玩家资格，返回后重新取得资格。
 *
 * <p>一个定时触发、至多一个合并的手动触发、两页扫描，以及每个已预留槽至多一个完成通知，
 * 是协调器队列的上界。槽保留到对应通知消费结束；不会靠丢弃回调或取消 Future 假报排空。
 * 扫描与在途各有字节预留，不把完整战斗输入/结果藏在“只计任务数”的队列里。</p>
 */
public final class BackgroundPump implements AutoCloseable {
    private final Object gate = new Object();
    private final RealmIdentity realm;
    private final RealmWorkReader reader;
    private final BoundedReadExecutor reads;
    private final ReadBudget budget;
    private final BackgroundWork work;
    private final BattleComputation compute;
    private final BooleanSupplier ready;
    private final Clock clock;
    private final BackgroundPumpLimits limits;
    private final BackgroundRetries retries;
    private final BackgroundRetries scanRetries;
    private final ScheduledThreadPoolExecutor coordinator;
    private final Map<String, Flight> flights = new LinkedHashMap<>();
    private final Set<Long> owners = new HashSet<>();
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private ScheduledFuture<?> heartbeat;
    private Thread coordinatorThread;
    private RealmWorkReader.DueCursor dueCursor;
    private String battleCursor;
    private long dueThrough;
    private boolean dueScanning, battleScanning, pollQueued, started, closing, closed, preferBattle;
    private boolean dueHealthy = true, battleHealthy = true;
    private volatile boolean dueObserved, battleObserved;
    private int tasks, battles, notifications;
    private long retainedBytes, submitted, settled, skipped, deferred, failures;
    private volatile Snapshot snapshot = new Snapshot(false, false, true, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

    public BackgroundPump(RealmIdentity realm, RealmWorkReader reader, BoundedReadExecutor reads, ReadBudget budget,
                          BackgroundWork work, BattleComputation compute, BooleanSupplier ready, Clock clock,
                          BackgroundPumpLimits limits) {
        this.realm = Objects.requireNonNull(realm); this.reader = Objects.requireNonNull(reader);
        this.reads = Objects.requireNonNull(reads); this.budget = Objects.requireNonNull(budget);
        this.work = Objects.requireNonNull(work); this.compute = Objects.requireNonNull(compute);
        this.ready = Objects.requireNonNull(ready); this.clock = Objects.requireNonNull(clock);
        this.limits = Objects.requireNonNull(limits); retries = new BackgroundRetries(limits); scanRetries = new BackgroundRetries(limits);
        if (budget.maxBytes() > limits.maxRetainedBytes() - BackgroundPumpLimits.BATTLE_SLOT_BYTES)
            throw new IllegalArgumentException("Background budget must hold a scan page and a battle slot together");
        coordinator = new ScheduledThreadPoolExecutor(1, action -> {
            var thread = Thread.ofPlatform().daemon().name("slg-background-coordinator").unstarted(action);
            coordinatorThread = thread; return thread;
        });
        coordinator.setRemoveOnCancelPolicy(true);
        coordinator.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

    /** 显式启动；构造不读取数据库或启动扫描，生产组合根决定何时已有有效写权并可恢复待办。 */
    public void start() {
        synchronized (gate) {
            if (started || closing) throw new IllegalStateException("Background pump already started or closing");
            started = true;
            heartbeat = coordinator.scheduleWithFixedDelay(this::pollOnce, 0,
                    limits.scanInterval().toNanos(), TimeUnit.NANOSECONDS);
        }
    }

    /** 只触发一次扫描，不等待 I/O；重复手动调用合并，不能把调度器变成外部无界任务队列。 */
    public boolean pollOnce() {
        synchronized (gate) {
            if (closing || pollQueued || dueScanning || battleScanning || !canDiscover()) return false;
            pollQueued = true;
            coordinator.execute(this::poll);
            return true;
        }
    }

    private void poll() {
        synchronized (gate) {
            pollQueued = false;
            if (!closing && canDiscover()) {
                // 两类后台工作轮换先申请页预算，不能让到期洪峰永久挤掉待计算战斗。
                if (preferBattle) { scanBattles(); scanDue(); }
                else { scanDue(); scanBattles(); }
                preferBattle = !preferBattle;
            }
            publishSnapshot(); completeDrain();
        }
    }

    private boolean canDiscover() {
        long now = System.nanoTime();
        return ready.getAsBoolean() && scanRetries.eligible("scan:due", now) && scanRetries.eligible("scan:battle", now);
    }

    private boolean canDispatch() { return !closing && dueHealthy && battleHealthy && canDiscover(); }
    private boolean fits(long bytes) { return bytes <= limits.maxRetainedBytes() - retainedBytes; }

    private void scanDue() {
        if (tasks >= limits.taskCapacity() || !fits(budget.maxBytes() + BackgroundPumpLimits.TASK_SLOT_BYTES)) return;
        if (dueCursor == null) dueThrough = Math.max(0, clock.millis());
        dueScanning = true; retainedBytes += budget.maxBytes() + BackgroundPumpLimits.TASK_SLOT_BYTES;
        try {
            reads.readDueTasks(reader, realm, dueThrough, dueCursor, limits.pageSize(), budget).result()
                    .whenComplete((page, failure) -> enqueue(() -> dueScanned(page, failure)));
        } catch (Throwable failure) { enqueue(() -> dueScanned(null, failure)); }
    }

    private void scanBattles() {
        if (battles >= limits.battleCapacity() || !fits(budget.maxBytes() + BackgroundPumpLimits.BATTLE_SLOT_BYTES)) return;
        battleScanning = true; retainedBytes += budget.maxBytes() + BackgroundPumpLimits.BATTLE_SLOT_BYTES;
        try {
            reads.readPendingBattles(reader, realm, battleCursor, limits.pageSize(), budget).result()
                    .whenComplete((page, failure) -> enqueue(() -> battlesScanned(page, failure)));
        } catch (Throwable failure) { enqueue(() -> battlesScanned(null, failure)); }
    }

    private void dueScanned(List<RealmWorkReader.DueTask> page, Throwable failure) {
        // 页读取期间也预留至少一个可接收槽，避免页本身耗光容量造成永久无法派发。协调器此刻
        // 独占gate，将该预留转给实际flight或释放；其他扫描不能插入这段转移过程。
        retainedBytes -= BackgroundPumpLimits.TASK_SLOT_BYTES;
        try {
            if (failure != null) { scanFailed("scan:due"); dueHealthy = false; return; }
            dueHealthy = true; scanRetries.success("scan:due");
            if (!canDispatch()) return;
            if (page.isEmpty()) { dueCursor = null; dueObserved = true; return; }
            for (var candidate : page) {
                if (tasks >= limits.taskCapacity() || !fits(BackgroundPumpLimits.TASK_SLOT_BYTES)) break;
                var next = candidate.cursor();
                if (next.dueAt() > dueThrough || !after(next, dueCursor))
                    throw new IllegalStateException("Due page is outside its fixed ordered scan");
                dueCursor = next;
                long account = PlayerIdentity.accountOf(candidate.task().playerId());
                String key = "due:" + candidate.task().taskId();
                if (!eligible(key, account)) { deferred++; continue; }
                var flight = reserve(key, account, candidate);
                try {
                    work.advance(candidate, limits.operationWait()).result()
                            .whenComplete((result, error) -> enqueue(() -> finished(flight, result, error, false)));
                    submitted++;
                } catch (Throwable error) { finished(flight, null, error, false); }
            }
            // 短页可能由字节预算造成，只有下一次真正读到空页才结束本轮固定 dueThrough。
            dueObserved = true;
        } catch (Throwable invalid) { scanFailed("scan:due"); dueHealthy = false; }
        finally { dueScanning = false; retainedBytes -= budget.maxBytes(); }
    }

    private void battlesScanned(List<RealmWorkReader.PendingBattle> page, Throwable failure) {
        retainedBytes -= BackgroundPumpLimits.BATTLE_SLOT_BYTES;
        try {
            if (failure != null) { scanFailed("scan:battle"); battleHealthy = false; return; }
            battleHealthy = true; scanRetries.success("scan:battle");
            if (!canDispatch()) return;
            if (page.isEmpty()) { battleCursor = null; battleObserved = true; return; }
            for (var candidate : page) {
                if (battles >= limits.battleCapacity() || !fits(BackgroundPumpLimits.BATTLE_SLOT_BYTES)) break;
                String id = candidate.battle().battleId();
                if (battleCursor != null && id.compareTo(battleCursor) <= 0)
                    throw new IllegalStateException("Battle page is not in exclusive cursor order");
                battleCursor = id;
                long account = PlayerIdentity.accountOf(candidate.battle().playerId());
                String key = "battle:" + id;
                if (!eligible(key, account)) { deferred++; continue; }
                var flight = reserve(key, account, candidate);
                try {
                    work.prepareBattle(candidate, limits.operationWait()).result()
                            .whenComplete((dispatch, error) -> enqueue(() -> prepared(flight, dispatch, error)));
                    submitted++;
                } catch (Throwable error) { finished(flight, null, error, false); }
            }
            battleObserved = true;
        } catch (Throwable invalid) { scanFailed("scan:battle"); battleHealthy = false; }
        finally { battleScanning = false; retainedBytes -= budget.maxBytes(); }
    }

    private boolean eligible(String key, long account) {
        return !flights.containsKey(key) && !owners.contains(account) && retries.eligible(key, System.nanoTime());
    }

    private Flight reserve(String key, long account, Object candidate) {
        boolean battle = candidate instanceof RealmWorkReader.PendingBattle;
        var flight = new Flight(key, account, candidate, battle ? BackgroundPumpLimits.BATTLE_SLOT_BYTES : BackgroundPumpLimits.TASK_SLOT_BYTES);
        flights.put(key, flight); owners.add(account); retainedBytes += flight.bytes;
        if (battle) battles++; else tasks++;
        return flight;
    }

    private void prepared(Flight flight, Optional<BattleDispatch> dispatch, Throwable error) {
        if (error != null) { finished(flight, null, error, false); return; }
        if (dispatch == null) { finished(flight, null, new IllegalArgumentException("Missing battle preparation outcome"), false); return; }
        if (dispatch.isEmpty()) { finished(flight, null, null, true); return; }
        if (!canDispatch()) { finished(flight, null, null, false); return; }
        try {
            var value = dispatch.orElseThrow();
            if (!value.candidate().battle().battleId().equals(((RealmWorkReader.PendingBattle) flight.candidate).battle().battleId())
                    || PlayerIdentity.accountOf(value.candidate().battle().playerId()) != flight.account)
                throw new IllegalArgumentException("Prepared battle differs from admitted identity");
            flight.candidate = value.candidate();
            // prepare 的内部通知已释放资格/pin；远程计算仅占当前独立battle slot。
            compute.compute(value.input(), value.candidate().battle().inputHash(), value.candidate().battle().rulesVersion())
                    .whenComplete((result, failure) -> enqueue(() -> computed(flight, result, failure)));
        } catch (Throwable failure) { finished(flight, null, failure, false); }
    }

    private void computed(Flight flight, BattleResult result, Throwable error) {
        if (error != null) { finished(flight, null, error, false); return; }
        if (closing || !canDispatch()) {
            // 尚未启动任何结算 SQL。丢弃这份纯计算结果安全，权威 PENDING 仍由恢复扫描重新派发。
            finished(flight, null, null, false); return;
        }
        try {
            work.settle((RealmWorkReader.PendingBattle) flight.candidate, Objects.requireNonNull(result), limits.operationWait()).result()
                    .whenComplete((outcome, failure) -> enqueue(() -> finished(flight, outcome, failure, false)));
        } catch (Throwable failure) { finished(flight, null, failure, false); }
    }

    private void finished(Flight flight, CommitResult result, Throwable failure, boolean stale) {
        if (flights.get(flight.key) != flight) throw new IllegalStateException("Background slot completed twice");
        boolean superseded = result instanceof CommitResult.NotCommitted rejected
                && rejected.reason() == CommitResult.Reason.VERSION_CONFLICT;
        if (failure == null && (stale || superseded || result instanceof CommitResult.Committed)) {
            retries.success(flight.key);
            if (result instanceof CommitResult.Committed committed && committed.applied()) settled++; else skipped++;
        } else {
            retries.fail(flight.key, System.nanoTime());
            if (failure == null) deferred++; else failures++;
        }
        flights.remove(flight.key); owners.remove(flight.account); retainedBytes -= flight.bytes;
        if (flight.candidate instanceof RealmWorkReader.PendingBattle) battles--; else tasks--;
        flight.candidate = null;
    }

    private void scanFailed(String key) { scanRetries.fail(key, System.nanoTime()); failures++; }

    /** 回调只排入协调器；每个扫描或slot保留到该通知消费结束，因此不会产生无界完成队列。 */
    private void enqueue(Runnable action) {
        synchronized (gate) {
            if (closed) throw new IllegalStateException("Background completion arrived after real drain");
            notifications++;
            coordinator.execute(() -> {
                synchronized (gate) {
                    try { action.run(); }
                    finally { notifications--; publishSnapshot(); completeDrain(); }
                }
            });
        }
    }

    /** 停止发现，真实读/计算/已开始结算保持计费；未知提交会使此stage继续等待，不能取消它。 */
    public CompletionStage<Void> drain() {
        synchronized (gate) {
            closing = true;
            if (heartbeat != null) heartbeat.cancel(false);
            publishSnapshot(); completeDrain();
            return drained.minimalCompletionStage();
        }
    }

    private void completeDrain() {
        if (!closing || closed || pollQueued || dueScanning || battleScanning || !flights.isEmpty() || notifications != 0) return;
        closed = true; publishSnapshot(); coordinator.shutdown(); drained.complete(null);
    }

    public Snapshot snapshot() { return snapshot; }
    /**
     * 启动恢复屏障只要求两类持久来源各完成一次合法有界发现，不等待积压全部清空。
     * 客户端仍与已投递工作共享资格；长期到期任务不能让区服永远无法接流。
     */
    public boolean initialScanComplete() { return dueObserved && battleObserved; }
    public boolean healthy() { return snapshot.scanHealthy() && ready.getAsBoolean(); }
    private void publishSnapshot() {
        snapshot = new Snapshot(closing, closed, dueHealthy && battleHealthy, (dueScanning ? 1 : 0) + (battleScanning ? 1 : 0),
                tasks, battles, retainedBytes, submitted, settled, skipped, deferred, failures, retries.size() + scanRetries.size(), notifications);
    }
    public record Snapshot(boolean closing, boolean closed, boolean scanHealthy, int scans, int taskInFlight,
                           int battleInFlight, long retainedBytes, long submitted, long settled, long skipped,
                           long deferred, long failures, int retryEntries, int queuedNotifications) { }

    @Override public void close() {
        if (Thread.currentThread() == coordinatorThread) throw new IllegalStateException("Cannot await background drain on its coordinator");
        try { drain().toCompletableFuture().get(limits.closeTimeout().toNanos(), TimeUnit.NANOSECONDS); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IllegalStateException("Background drain interrupted", interrupted); }
        catch (TimeoutException timeout) { throw new IllegalStateException("Background drain incomplete; reads, Worker or settlement remain active", timeout); }
        catch (ExecutionException failure) { throw new IllegalStateException("Background drain failed", failure.getCause()); }
    }

    private static boolean after(RealmWorkReader.DueCursor value, RealmWorkReader.DueCursor prior) {
        return prior == null || value.dueAt() > prior.dueAt()
                || value.dueAt() == prior.dueAt() && value.taskId().compareTo(prior.taskId()) > 0;
    }
    private static final class Flight {
        final String key;
        final long account, bytes;
        Object candidate;
        Flight(String key, long account, Object candidate, long bytes) {
            this.key = key; this.account = account; this.candidate = candidate; this.bytes = bytes;
        }
    }
}
