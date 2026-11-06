package com.slg.game.realm;

import com.slg.game.storage.RealmJournal;
import com.slg.game.storage.RealmJournal.DueTask;
import java.io.IOException;
import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * 按持久化的到期时间发现任务。扫描只投递给玩家邮箱；在途集合用来减少重复请求，
 * 一次生效的依据是原卷中的任务状态、代次和系统回执。
 */
public final class RealmTaskPump implements AutoCloseable {
    private static final System.Logger LOG = System.getLogger(RealmTaskPump.class.getName());
    private final RealmJournal journal;
    private final ActivePlayers players;
    private final RealmMarchService service;
    private final Clock clock;
    private final int batchSize;
    private final ScheduledExecutorService poller = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("realm-due-scan").factory());
    private final Set<String> inFlightOwners = ConcurrentHashMap.newKeySet();
    private final Map<String, Integer> inFlightTargets = new ConcurrentHashMap<>();
    private final LongAdder submitted = new LongAdder(), applied = new LongAdder(), skipped = new LongAdder();
    private final LongAdder deferred = new LongAdder(), failures = new LongAdder();
    private volatile boolean scanHealthy = true;
    private volatile String lastFailedTask = "";
    private boolean started, closed, moreDue;
    private DueTask cursor;
    private long scanThrough;

    public RealmTaskPump(RealmJournal journal, ActivePlayers players, RealmMarchService service, Clock clock, int batchSize) {
        this.journal = Objects.requireNonNull(journal);
        this.players = Objects.requireNonNull(players);
        this.service = Objects.requireNonNull(service);
        this.clock = Objects.requireNonNull(clock);
        if (batchSize < 1 || batchSize > 1024) throw new IllegalArgumentException("Invalid due task batch size");
        this.batchSize = batchSize;
    }

    public synchronized void start() throws IOException {
        if (started || closed) throw new IllegalStateException("Due scanner already started or closed");
        poll(); // 校验能读索引再开放就绪；追赶积压在后台分批继续。
        started = true;
        scheduleNext();
    }

    private synchronized void tick() {
        if (closed) return;
        try { poll(); }
        catch (IOException | RuntimeException failure) {
            if (scanHealthy) LOG.log(System.Logger.Level.ERROR, "Cannot scan due realm tasks", failure);
            scanHealthy = false;
        }
        scheduleNext();
    }

    private void scheduleNext() {
        // 扫满一页时快速翻页，与固定心跳解耦。存储错误退避，闲时避免反复读盘。
        long delayMs = !scanHealthy ? 1000 : moreDue ? 5 : 100;
        poller.schedule(this::tick, delayMs, TimeUnit.MILLISECONDS);
    }

    /** 每轮最多读一页、每个玩家最多一项在途；即使热点账号拒绝受理，也继续向后翻页。 */
    public synchronized int poll() throws IOException {
        if (closed || inFlightOwners.size() >= batchSize) return 0;
        long now = clock.millis();
        // 固定一轮扫描的时间上界；持续新增的到期任务不能让游标永远追不上队尾，
        // 否则本轮早先遇到队列满的玩家可能一直得不到第二次机会。
        if (cursor == null) scanThrough = now;
        var page = journal.dueTasks(Math.min(now, scanThrough), cursor, batchSize);
        moreDue = page.size() == batchSize;
        scanHealthy = true;
        int offered = 0;
        Set<String> visited = new HashSet<>();
        for (DueTask task : page) {
            if (inFlightOwners.size() >= batchSize) return offered; // 下次从最后检查的位置继续。
            cursor = task;
            if (!visited.add(task.playerId()) || !inFlightOwners.add(task.playerId())) continue;
            String target = null;
            try {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                // 到期索引返回 (dueAt, taskId) 顺序。在扫描线程取目标，先登记本次投递的矿点，再让
                // 玩家冷加载或等待邮箱；不会因玩家加载耗时不同而颠倒两个已受理的同矿到达。
                target = switch (task.type()) {
                    case "MARCH_ARRIVE", "GATHER_DONE" -> MarchExecutionTarget.forMarch(journal, task.playerId(), task.marchId());
                    default -> null;
                };
                // 本批次至少给其他目标留下名额（批大小为 1 时仍允许一道到达）。
                // 跳过的待办保持 PENDING，游标继续向后扫描，避免一个排队矿占满全部发现名额。
                if (target != null && inFlightTargets.merge(target, 1, Integer::sum) > (batchSize + 1) / 2) {
                    releaseOffer(task.playerId(), target);
                    deferred.increment();
                    continue;
                }
                String registeredTarget = target;
                // 原卷保存角色 ID；邮箱以认证账号为键。两类入口必须在同一邮箱相遇，否则返城会与派兵并发。
                String account = PlayerIdentity.accountOf(task.playerId());
                var future = players.submitForTarget(account, deadline, target, player -> service.applyDue(player, task));
                submitted.increment();
                offered++;
                future.whenComplete((changed, error) -> {
                    try {
                        if (error == null) {
                            if (Boolean.TRUE.equals(changed)) applied.increment(); else skipped.increment();
                        } else if (error instanceof RejectedExecutionException || error instanceof QueueDeadlineException) {
                            deferred.increment(); // 只延后，原卷仍为 PENDING。
                        } else {
                            failures.increment();
                            if (!task.taskId().equals(lastFailedTask))
                                LOG.log(System.Logger.Level.WARNING, "Realm task failed: " + task.taskId(), error);
                            lastFailedTask = task.taskId();
                        }
                    } finally { releaseOffer(task.playerId(), registeredTarget); }
                });
            } catch (IOException | RuntimeException e) {
                releaseOffer(task.playerId(), target);
                throw e;
            }
        }
        if (page.size() < batchSize) cursor = null;
        return offered;
    }

    /** 完成回调来自业务线程，不在扫描锁上等待下一页磁盘读取。 */
    private void releaseOffer(String owner, String target) {
        if (target != null)
            inFlightTargets.computeIfPresent(target, (key, count) -> count == 1 ? null : count - 1);
        inFlightOwners.remove(owner);
    }

    public boolean healthy() { return scanHealthy; }

    public Map<String, Object> metrics() {
        return Map.of("healthy", scanHealthy, "inFlightPlayers", inFlightOwners.size(),
                "submitted", submitted.sum(), "applied", applied.sum(), "skipped", skipped.sum(),
                "deferred", deferred.sum(), "failures", failures.sum(), "lastFailedTask", lastFailedTask);
    }

    /** 此方法先停止扫描；调用方随后关闭玩家目录并等待已投递的任务，再关闭区服卷。 */
    @Override public synchronized void close() {
        closed = true;
        poller.shutdown();
    }
}
