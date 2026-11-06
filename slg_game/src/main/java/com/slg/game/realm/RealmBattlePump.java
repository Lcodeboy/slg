package com.slg.game.realm;

import com.slg.core.battle.BattleInput;
import com.slg.core.battle.BattleResult;
import com.slg.game.storage.RealmJournal;
import com.slg.game.storage.RealmJournal.PendingBattle;
import io.grpc.Status;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** 从原卷发现 NPC 请求。远程计算期间不占用玩家邮箱，结果回到所属玩家邮箱后才能修改区服状态。 */
public final class RealmBattlePump implements AutoCloseable {
    @FunctionalInterface
    public interface Compute {
        CompletableFuture<BattleResult> run(BattleInput input, String digest, String rules);
    }

    private static final System.Logger LOG = System.getLogger(RealmBattlePump.class.getName());
    private final RealmJournal journal;
    private final ActivePlayers players;
    private final RealmMarchService service;
    private final Compute compute;
    private final int capacity;
    private final ScheduledExecutorService coordinator = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("realm-battle-dispatch").factory());
    private final Set<String> inFlight = new HashSet<>();
    private final LinkedHashMap<String, Retry> retries = new LinkedHashMap<>();
    private record Retry(int failures, long afterNanos) {}
    private String cursor, lastFailed = "";
    private boolean started, closed, scanQueued;
    private volatile boolean scanHealthy = true;
    private long submitted, settled, failed, lastWarning;
    private long resumeNanos = System.nanoTime();

    public RealmBattlePump(RealmJournal journal, ActivePlayers players, RealmMarchService service,
                           Compute compute, int capacity) {
        this.journal = Objects.requireNonNull(journal);
        this.players = Objects.requireNonNull(players);
        this.service = Objects.requireNonNull(service);
        this.compute = Objects.requireNonNull(compute);
        if (capacity < 1 || capacity > 64) throw new IllegalArgumentException("Invalid battle dispatch capacity");
        this.capacity = capacity;
    }

    public synchronized void start() throws IOException {
        if (closed || started) throw new IllegalStateException("Battle dispatcher already started or closed");
        poll();
        started = true;
        scanNextPage();
        coordinator.scheduleWithFixedDelay(this::tick, 100, 100, TimeUnit.MILLISECONDS);
    }

    private synchronized void tick() {
        if (closed) return;
        try {
            poll();
            scanNextPage();
        } catch (IOException | RuntimeException error) {
            if (scanHealthy) LOG.log(System.Logger.Level.ERROR, "Cannot scan realm battle index", error);
            scanHealthy = false;
        }
    }

    /** 有积压时在名额释放后立即翻页。空队列靠心跳发现新任务，同一时间最多排一个额外扫描。 */
    private synchronized void scanNextPage() {
        if (!started || closed || cursor == null || scanQueued || inFlight.size() >= capacity
                || System.nanoTime() - resumeNanos < 0) return;
        scanQueued = true;
        coordinator.execute(() -> {
            synchronized (this) {
                scanQueued = false;
                tick();
            }
        });
    }

    private void poll() throws IOException {
        long now = System.nanoTime();
        if (inFlight.size() >= capacity || now - resumeNanos < 0) return;
        var page = journal.pendingBattles(cursor, capacity);
        scanHealthy = true;
        for (PendingBattle job : page) {
            if (inFlight.size() >= capacity) return;
            cursor = job.battleId();
            Retry retry = retries.get(cursor);
            if ((retry != null && now - retry.afterNanos() < 0) || !inFlight.add(cursor)) continue;
            try {
                BattleInput input = service.battleInput(job.record(), job.battleId());
                submitted++;
                compute.run(input, job.inputHash(), job.rulesVersion()).whenComplete((result, error) ->
                        enqueue(() -> computed(job, result, error)));
            } catch (IOException | RuntimeException error) {
                finish(job, false, error);
            }
        }
        if (page.size() < capacity) cursor = null;
    }

    /** 回调线程仅通知协调器。关闭后的结果可丢弃，下次由原卷的待处理记录重新派发。 */
    private void enqueue(Runnable action) {
        try { coordinator.execute(action); }
        catch (RejectedExecutionException stopped) { /* 协调器已关闭。 */ }
    }

    private synchronized void computed(PendingBattle job, BattleResult result, Throwable error) {
        if (closed) return;
        if (error != null) {
            // 仅 Worker 连接或计算池普遍不可用时延缓全区派发。坏输入、坏战果和满员的玩家邮箱独立退避。
            var code = Status.fromThrowable(error).getCode();
            if (code == Status.Code.UNAVAILABLE || code == Status.Code.RESOURCE_EXHAUSTED) {
                resumeNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(250);
            }
            finish(job, false, error);
            return;
        }
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            // 同一账号的另一区角色拥有不同的角色 ID；进入本区邮箱后，settle 会再验证完整角色 ID。
            players.submit(PlayerIdentity.accountOf(job.playerId()), deadline, active ->
                    service.settleBattle(active, job.battleId(), job.inputHash(), job.rulesVersion(), result))
                    .whenComplete((changed, failure) -> enqueue(() -> finish(job, changed, failure)));
        } catch (RuntimeException rejected) {
            finish(job, false, rejected);
        }
    }

    private synchronized void finish(PendingBattle job, Boolean changed, Throwable error) {
        if (closed) return;
        String id = job.battleId();
        inFlight.remove(id);
        scanNextPage();
        if (error == null) {
            retries.remove(id);
            if (Boolean.TRUE.equals(changed)) settled++;
            return;
        }
        // 对同一失败任务指数退避；只缓存近期失败，待办自身始终留在原卷。
        long now = System.nanoTime();
        Retry previous = retries.remove(id);
        int failures = previous == null ? 1 : Math.min(6, previous.failures() + 1);
        if (retries.size() >= 1024) retries.pollFirstEntry();
        retries.put(id, new Retry(failures, now + TimeUnit.MILLISECONDS.toNanos(250L << (failures - 1))));
        if (failed == 0 || now - lastWarning >= TimeUnit.SECONDS.toNanos(10)) {
            LOG.log(System.Logger.Level.WARNING, "Retrying persisted NPC battle " + id, error);
            lastWarning = now;
        }
        lastFailed = id;
        failed++;
    }

    // gRPC 接入线程只读状态，不等待协调器读盘或提交工作。
    public boolean healthy() { return scanHealthy; }

    public synchronized Map<String, Object> metrics() {
        return Map.of("indexHealthy", scanHealthy, "capacity", capacity, "inFlight", inFlight.size(),
                "submitted", submitted, "settled", settled, "failedAttempts", failed, "lastFailedBattle", lastFailed);
    }

    @Override public synchronized void close() {
        closed = true;
        coordinator.shutdownNow();
    }
}
