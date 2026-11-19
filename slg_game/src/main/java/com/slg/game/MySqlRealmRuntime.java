package com.slg.game;

import com.slg.game.aoi.MySqlMapAccess;
import com.slg.game.aoi.MySqlMapStateSource;
import com.slg.game.application.background.*;
import com.slg.game.application.commit.*;
import com.slg.game.application.march.*;
import com.slg.game.application.player.*;
import com.slg.game.application.port.*;
import com.slg.game.application.read.BoundedReadExecutor;
import com.slg.game.domain.persistence.RealmIdentity;
import com.slg.game.domain.persistence.WriterToken;
import com.slg.game.infrastructure.battle.GrpcBattleComputation;
import com.slg.game.infrastructure.mysql.*;
import com.slg.game.infrastructure.mysql.codec.*;
import com.slg.game.infrastructure.mysql.metrics.MySqlStorageMetrics;
import com.slg.game.infrastructure.mysql.runtime.*;
import com.slg.rpc.BattleWorkerClient;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 单区 MySQL 权威资源组合根；不绑定 HTTP/gRPC、不初始化库、不启动旧原卷或投影。
 * 进程外层先关闭网络和地图读取，随后关闭本运行时。每层必须真实排空后才关闭下游依赖；
 * UNKNOWN、发布隔离或关闭超时会保留资源和阶段，不能为了退出速度提前释放写权/连接池。
 */
final class MySqlRealmRuntime implements AutoCloseable {
    private enum CloseStep { BACKGROUND, MAINTENANCE, APPLICATION, COMMIT, READS, AUXILIARY, OWNERSHIP, DATABASE, CLOSED }
    private final RealmIdentity realm;
    private final RealmRuntimeSettings settings;
    private final RealmDatabaseReadiness databaseReady;
    private final MySqlStorageMetrics storageMetrics = new MySqlStorageMetrics();
    private final AtomicBoolean stopped = new AtomicBoolean();
    private volatile boolean started;
    private volatile CloseStep closeStep = CloseStep.BACKGROUND;
    private long startupNanos;
    private AuthorityDataSources pools;
    private MySqlStartupCheck startupCheck;
    private RealmOwnership ownership;
    private WriterToken acquiredWriter;
    private BoundedRealmCommitService commits;
    private BoundedReadExecutor reads;
    private PlayerDirectory directory;
    private SessionDirectory sessions;
    private PlayerApplication players;
    private MarchApplication marches;
    private BackgroundApplication background;
    private BackgroundPump pump;
    private BattleWorkerClient worker;
    private MySqlMapStateSource mapSource;
    private MySqlMapAccess mapAccess;
    private ScheduledThreadPoolExecutor maintenance;

    private MySqlRealmRuntime(RealmIdentity realm, RealmRuntimeSettings settings) {
        this.realm = Objects.requireNonNull(realm); this.settings = Objects.requireNonNull(settings);
        databaseReady = new RealmDatabaseReadiness(settings.probeFreshness());
    }

    static MySqlRealmRuntime open(RealmIdentity realm, Map<String, String> env) throws Exception {
        Objects.requireNonNull(realm); Objects.requireNonNull(env);
        long began = System.nanoTime();
        var database = AuthorityDatabaseSettings.fromEnvironment(env);
        var runtime = new MySqlRealmRuntime(realm, RealmRuntimeSettings.fromEnvironment(env, database));
        try {
            runtime.initialize(database);
            runtime.startupNanos = System.nanoTime() - began;
            return runtime;
        } catch (Exception | Error failure) {
            // acquire 确认丢失时没有猜测 token 可释放；停止所有本地资源后允许数据库租约自然失效。
            // 已取得 token 时则按相同排空顺序释放。清理失败不继续关依赖，也不覆盖原启动失败。
            try { runtime.close(); } catch (Exception | Error cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private void initialize(AuthorityDatabaseSettings database) throws Exception {
        pools = AuthorityDataSources.open(database);
        startupCheck = new MySqlStartupCheck(pools.control(), database.statementTimeoutSeconds());
        startupCheck.verify(realm);
        // 端口共享既有实现，但四类连接池物理分离；地图或冷读排队不能占满续期/fence 的连接。
        var writes = new MySqlRealmStorage(pools.tx(), database.statementTimeoutSeconds(), settings.leaseDuration(), storageMetrics);
        var cold = new MySqlRealmStorage(pools.read(), database.statementTimeoutSeconds(), settings.leaseDuration(), storageMetrics);
        var map = new MySqlRealmStorage(pools.map(), database.statementTimeoutSeconds(), settings.leaseDuration(), storageMetrics);
        var control = new MySqlRealmStorage(pools.control(), database.statementTimeoutSeconds(), settings.leaseDuration(), storageMetrics);
        ownership = control.ownership();
        acquiredWriter = ownership.acquire(realm, UUID.randomUUID());
        var sizer = new MySqlCommitPayloadSizer(new PersistenceCodec());
        // acquire 的排他屏障已等待旧任期在途事务；提交器从此唯一负责续期、UNKNOWN fence 和核对。
        commits = new BoundedRealmCommitService(acquiredWriter, writes.transactions(), ownership, sizer, settings.commits());
        reads = new BoundedReadExecutor(settings.reads());
        var playerCodec = new PlayerPayloadJsonCodec();
        var marchCodec = new MarchPayloadJsonCodec();
        var backgroundCodec = new BackgroundJsonCodec();
        var clock = Clock.systemUTC();
        directory = new PlayerDirectory(realm, PlayerSnapshotLoader.using(realm, cold.states(), reads, settings.playerRead()),
                settings.cache(), playerCodec::measureSnapshot);
        sessions = new SessionDirectory(clock, settings.sessionLimit());
        players = new PlayerApplication(realm, directory, cold.receipts(), reads, settings.playerRead(), playerCodec,
                PlayerSessionAdapter.access(sessions, this::ready), commits, sizer, settings.operations(),
                PlayerSessionAdapter.controlHandlers(sessions, this::ready),
                List.of(new MarchModule(realm, cold.states(), reads, settings.playerRead(), marchCodec, clock),
                        new BackgroundModule(realm, cold.states(), reads, settings.playerRead(), backgroundCodec, marchCodec, clock)));
        marches = new MarchApplication(players, realm, marchCodec);
        background = new BackgroundApplication(players, realm, backgroundCodec, playerCodec);
        worker = new BattleWorkerClient(settings.workerTarget());
        pump = new BackgroundPump(realm, cold.work(), reads, settings.backgroundRead(), background,
                new GrpcBattleComputation(worker), this::backgroundReady, clock, settings.pump());
        mapSource = new MySqlMapStateSource(map.states(), map.events(), realm, settings.mapSnapshotRead(), settings.mapEventRead());
        mapAccess = new MySqlMapAccess(map.states(), realm, settings.mapAccessRead());
        maintenance = new ScheduledThreadPoolExecutor(1,
                Thread.ofPlatform().daemon().name("slg-realm-maintenance-" + realm.realmId()).factory());
        maintenance.setRemoveOnCancelPolicy(true);
        maintenance.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        maintenance.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        long began = System.nanoTime();
        databaseReady.observed(startupCheck.probe(realm), began);
        if (!databaseReady.allows(commits.snapshot().writer()))
            throw new StorageAccessException(StorageAccessException.Code.OWNERSHIP_CONFLICT, "Startup owner observation is not current");
    }

    /** 启动有限恢复发现；客户端须等两类来源首次成功扫描，不能靠端口已绑定宣称恢复完成。 */
    synchronized void start() throws Exception {
        if (started || stopped.get()) throw new IllegalStateException("Realm runtime already started or stopping");
        started = true;
        try {
            pump.start();
            // 固定一个周期任务；probe 同步执行且不会重叠。目录有总条目上限，不安排每玩家定时器。
            maintenance.scheduleWithFixedDelay(this::maintain, 0, settings.probeInterval().toNanos(), TimeUnit.NANOSECONDS);
        } catch (RuntimeException | Error failure) {
            // 此时外层可能已启动 AOI/地图读取；这里只摘流，由进程先排空传输再关闭本运行时。
            stopAccepting();
            throw failure;
        }
    }

    private void maintain() {
        if (stopped.get()) return;
        try {
            directory.evictIdle();
            if (!databaseReady.schemaValid()) return;
            long began = System.nanoTime();
            databaseReady.observed(startupCheck.probe(realm), began);
            if (!stopped.get()) pump.pollOnce();
        } catch (StorageAccessException failure) { databaseReady.failed(failure.code()); }
        catch (RuntimeException failure) { databaseReady.failed(StorageAccessException.Code.UNAVAILABLE); }
    }

    /** 背景准入不能依赖客户端 ready，否则初始恢复扫描与接流屏障会形成循环等待。 */
    boolean backgroundReady() {
        if (!started || stopped.get() || commits == null) return false;
        var state = commits.snapshot();
        return state.mode() == CommitQueueSnapshot.Mode.ACTIVE && databaseReady.allows(state.writer());
    }

    boolean ready() {
        return backgroundReady() && pump != null && pump.initialScanComplete() && pump.snapshot().scanHealthy();
    }

    /** 外层关闭前同步摘流；这个闸门永久关闭，不会被后续探测成功或 UNKNOWN 核对重新打开。 */
    void stopAccepting() { stopped.set(true); }

    PlayerApplication players() { return players; }
    MarchApplication marches() { return marches; }
    BackgroundApplication background() { return background; }
    SessionDirectory sessions() { return sessions; }
    MySqlMapStateSource mapSource() { return mapSource; }
    MySqlMapAccess mapAccess() { return mapAccess; }
    RealmRuntimeSettings settings() { return settings; }
    long startupNanos() { return startupNanos; }
    Map<String, Object> connections() { return pools == null ? Map.of() : pools.metrics(); }

    /** 健康查询只读取内存观察和队列计数，不占 SQL/玩家资格，也不泄漏 JDBC 地址或凭据。 */
    Map<String, Object> health() {
        var result = new LinkedHashMap<String, Object>();
        boolean accepting = ready();
        result.put("state", closeStep == CloseStep.CLOSED ? "CLOSED" : stopped.get() ? "STOPPING"
                : !databaseReady.schemaValid() ? "INVALID_DATABASE" : accepting ? "READY" : "RECOVERING");
        result.put("ready", accepting); result.put("backgroundReady", backgroundReady());
        result.put("realmId", realm.realmId()); result.put("datasetId", realm.datasetId().toString());
        result.put("recoveryScanned", pump != null && pump.initialScanComplete());
        var observation = databaseReady.health();
        result.put("database", observation); result.put("connections", connections());
        result.put("highSequence", observation.getOrDefault("highSequence", -1L));
        result.put("owner", observation);
        result.put("storage", storageMetrics.snapshot());
        result.put("closeStep", closeStep.name());
        if (commits != null) result.put("commit", commits.snapshot());
        if (players != null) result.put("operations", players.stats());
        if (reads != null) result.put("reads", reads.statistics());
        if (directory != null) result.put("players", directory.statistics());
        if (pump != null) result.put("background", pump.snapshot());
        return Map.copyOf(result);
    }

    /**
     * 可重入的分阶段关闭：只有本阶段确认完成才前移。超时不取消真实 Future、不丢引用，
     * 再次 close 从未完成阶段继续；若 UNKNOWN 永不收敛，下游保持可用直到外层结束进程。
     */
    @Override public synchronized void close() throws Exception {
        stopAccepting();
        if (maintenance != null) maintenance.shutdown(); // 不 interrupt 正在持控制连接的 probe。
        while (closeStep != CloseStep.CLOSED) {
            switch (closeStep) {
                case BACKGROUND -> {
                    if (pump != null) await(pump.drain(), "background work");
                    closeStep = CloseStep.MAINTENANCE;
                }
                case MAINTENANCE -> {
                    if (maintenance != null && !maintenance.awaitTermination(settings.closeTimeout().toNanos(), TimeUnit.NANOSECONDS))
                        throw new IllegalStateException("Realm probe has not drained; control connections remain open");
                    closeStep = CloseStep.APPLICATION;
                }
                case APPLICATION -> {
                    if (players != null) await(players.drain(), "player operations and cache");
                    else if (directory != null) await(directory.drain(), "player cache");
                    closeStep = CloseStep.COMMIT;
                }
                case COMMIT -> {
                    if (commits != null) await(commits.drain(), "commits and UNKNOWN reconciliation");
                    closeStep = CloseStep.READS;
                }
                case READS -> {
                    if (reads != null) await(reads.drain(), "cold reads and notifications");
                    closeStep = CloseStep.AUXILIARY;
                }
                case AUXILIARY -> {
                    if (sessions != null) sessions.close();
                    // pump 先证明真 Worker 和结果通知结束，之后才允许关闭远程传输。
                    if (worker != null) worker.close();
                    closeStep = CloseStep.OWNERSHIP;
                }
                case OWNERSHIP -> {
                    WriterToken writer = commits == null ? acquiredWriter : commits.snapshot().writer();
                    if (ownership != null && writer != null) ownership.release(writer);
                    closeStep = CloseStep.DATABASE;
                }
                case DATABASE -> {
                    if (pools != null) pools.close();
                    closeStep = CloseStep.CLOSED;
                }
                case CLOSED -> { }
            }
        }
    }

    private void await(CompletionStage<Void> stage, String name) throws Exception {
        try { stage.toCompletableFuture().get(settings.closeTimeout().toNanos(), TimeUnit.NANOSECONDS); }
        catch (TimeoutException timeout) { throw new IllegalStateException("Realm has not drained " + name + "; dependencies remain open", timeout); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw interrupted; }
        catch (ExecutionException failure) { throw new IllegalStateException("Realm drain failed for " + name, failure.getCause()); }
    }
}
