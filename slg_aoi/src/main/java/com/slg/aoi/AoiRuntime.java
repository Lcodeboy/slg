package com.slg.aoi;

import com.slg.aoi.MapStateSource.*;
import com.slg.core.world.Point;
import com.slg.core.world.RouteIndex;
import com.slg.core.world.Viewport;
import com.slg.core.metrics.LatencyMetric;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * GAME 内的 AOI 运行时。权威写入不等待本组件：提交信号仅唤醒，正确性来自固定快照和连续日志。
 * 数据读取、串行发布、查询和视野输出各有独立预算，所有外部操作都通过只读接口和不可变 DTO。
 * 本类没有数据库句柄、网络协议或玩家实体依赖，未来拆进程时只替换两个边界适配器。
 */
public final class AoiRuntime implements AutoCloseable {
    private static final System.Logger LOG = System.getLogger(AoiRuntime.class.getName());

    public record View(Identity identity, String generation, long appliedSeq, List<Route> routes, List<WorldObject> objects) {
        public View { routes = List.copyOf(routes); objects = List.copyOf(objects); }
    }

    /** snapshot=true 时先清空该订阅的旧视图；其后只接受同 generation 且 fromSeq 衔接的增量。 */
    public record Update(Identity identity, String generation, long fromSeq, long throughSeq,
                         boolean snapshot, List<Route> upserts, List<String> removedIds,
                         List<WorldObject> objectUpserts, List<String> removedObjectIds) {
        public Update {
            upserts = List.copyOf(upserts); removedIds = List.copyOf(removedIds);
            objectUpserts = List.copyOf(objectUpserts); removedObjectIds = List.copyOf(removedObjectIds);
        }
    }

    /** 两个回调都必须非阻塞。offer=false 表示下游已满；调用方应结束旧订阅并重新获取快照。 */
    public interface ViewUpdateSink {
        boolean offer(Update update);
        void resetRequired(String reason);
    }

    public interface Subscription extends AutoCloseable { @Override void close(); }

    public static final class Unavailable extends IllegalStateException {
        public Unavailable(String message) { super(message); }
    }

    public static final class Overloaded extends RejectedExecutionException {
        public Overloaded(String message) { super(message); }
    }

    /**
     * 常驻路线与点对象分别限量，删除版本、查询候选/结果和订阅引用按所有类型合计。
     * 一次重建最多同时保留两份索引，部署内存预算必须包含旧视图、重建视图和 JVM/原生存储开销。
     */
    public record Settings(int mapSize, int chunkSize, int maxRoutes, int maxObjects, int maxTrackedVersions,
                           int maxCandidates, int maxResults, int queryThreads, int queryQueueCapacity,
                           int maxSubscriptions, int subscriptionQueueCapacity, int maxSubscriptionEntries,
                           int deliveryThreads, int maxPendingUpdates, int pageSize,
                           int pollMillis, int readTimeoutMillis) {
        public Settings {
            if (mapSize < 2 || mapSize > 100_000 || chunkSize < 1 || chunkSize > mapSize
                    || maxRoutes < 1 || maxRoutes > 1_000_000 || maxObjects < 1 || maxObjects > 1_000_000
                    || maxTrackedVersions < Math.max(maxRoutes, maxObjects)
                    || maxTrackedVersions > 2_000_000 || maxCandidates < 1 || maxCandidates > (long) maxRoutes + maxObjects
                    || maxResults < 1 || maxResults > maxCandidates || queryThreads < 1 || queryThreads > 32
                    || queryQueueCapacity < 1 || queryQueueCapacity > 8192 || maxSubscriptions < 1
                    || maxSubscriptions > 100_000 || subscriptionQueueCapacity < 1 || subscriptionQueueCapacity > 128
                    || maxSubscriptionEntries < 1 || maxSubscriptionEntries > 2_000_000
                    || deliveryThreads < 1 || deliveryThreads > 32 || maxPendingUpdates < 1 || maxPendingUpdates > 8192
                    || pageSize < 1 || pageSize > 128 || pollMillis < 1 || pollMillis > 10_000
                    || readTimeoutMillis < 1 || readTimeoutMillis > 300_000)
                throw new IllegalArgumentException("Invalid AOI budgets");
        }
        public static Settings defaults() {
            return new Settings(1200, 32, 100_000, 100_000, 200_000, 8192, 1024,
                    2, 128, 4096, 8, 200_000, 2, 2048, 128, 100, 60_000);
        }
        /** 不读取全局环境，便于启动器预检与隔离测试使用同一配置规则。 */
        public static Settings fromEnvironment(Map<String, String> env) {
            Settings d = defaults();
            return new Settings(d.mapSize, d.chunkSize,
                    setting(env, "MAX_ROUTES", d.maxRoutes), setting(env, "MAX_OBJECTS", d.maxObjects),
                    setting(env, "MAX_TRACKED_VERSIONS", d.maxTrackedVersions),
                    setting(env, "MAX_CANDIDATES", d.maxCandidates), setting(env, "MAX_RESULTS", d.maxResults),
                    setting(env, "QUERY_THREADS", d.queryThreads), setting(env, "QUERY_QUEUE", d.queryQueueCapacity),
                    setting(env, "MAX_SUBSCRIPTIONS", d.maxSubscriptions), setting(env, "SUBSCRIPTION_QUEUE", d.subscriptionQueueCapacity),
                    setting(env, "MAX_SUBSCRIPTION_ENTRIES", d.maxSubscriptionEntries), setting(env, "DELIVERY_THREADS", d.deliveryThreads),
                    setting(env, "MAX_PENDING_UPDATES", d.maxPendingUpdates), setting(env, "PAGE_SIZE", d.pageSize),
                    setting(env, "POLL_MS", d.pollMillis), setting(env, "READ_TIMEOUT_MS", d.readTimeoutMillis));
        }
        private static int setting(Map<String, String> env, String key, int fallback) {
            return Integer.parseInt(env.getOrDefault("SLG_AOI_" + key, Integer.toString(fallback)));
        }
    }

    private final MapStateSource source;
    private final Identity identity;
    private final Settings settings;
    // ScheduledThreadPoolExecutor 的内部队列本身无界。这里只允许一个合并信号、一个周期任务、
    // 一项读取完成通知、每个已注册订阅至多一项拒绝回调，以及经 externalControls 限额的订阅命令，
    // 不能直接暴露 execute 给调用方。
    private final ScheduledThreadPoolExecutor control = new ScheduledThreadPoolExecutor(1,
            Thread.ofPlatform().daemon().name("aoi-update-").factory());
    private final ThreadPoolExecutor reader;
    private final ThreadPoolExecutor queries;
    private final ThreadPoolExecutor deliveries;
    private final Semaphore externalControls;
    private final Semaphore pendingUpdates;
    private final AtomicBoolean started = new AtomicBoolean(), closed = new AtomicBoolean(), signalQueued = new AtomicBoolean();
    private final AtomicLong queryRejected = new AtomicLong(), resets = new AtomicLong(), rebuilds = new AtomicLong();
    private final LatencyMetric queryWait = new LatencyMetric(), queryExecution = new LatencyMetric();
    private final LatencyMetric snapshotBuild = new LatencyMetric(), deltaRead = new LatencyMetric(), publish = new LatencyMetric();
    private final Map<Long, Subscriber> subscribers = new HashMap<>(); // 只由 control 修改
    private final java.util.Set<CompletableFuture<?>> pendingCalls = ConcurrentHashMap.newKeySet();
    private volatile AoiIndex index;
    private volatile String state = "STARTING", failure = "";
    private volatile long sourceHigh;
    private volatile int subscriberCount;
    private AutoCloseable notifications;
    private boolean reading, rebuild = true, fatal;
    private long nextSubscription;
    private volatile int subscribedEntries;

    public AoiRuntime(MapStateSource source, Settings settings) {
        this.source = Objects.requireNonNull(source, "source");
        this.identity = Objects.requireNonNull(source.identity(), "identity");
        this.settings = Objects.requireNonNull(settings, "settings");
        reader = pool("aoi-source-", 1, 1);
        queries = pool("aoi-query-", settings.queryThreads(), settings.queryQueueCapacity());
        deliveries = pool("aoi-delivery-", settings.deliveryThreads(), settings.maxPendingUpdates());
        externalControls = new Semaphore(settings.queryQueueCapacity());
        pendingUpdates = new Semaphore(settings.maxPendingUpdates());
        control.setRemoveOnCancelPolicy(true);
        control.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        control.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    }

    private static ThreadPoolExecutor pool(String name, int threads, int capacity) {
        return new ThreadPoolExecutor(threads, threads, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(capacity),
                Thread.ofPlatform().daemon().name(name, 0).factory(), new ThreadPoolExecutor.AbortPolicy());
    }

    public void start() throws Exception {
        if (closed.get()) throw new IllegalStateException("AOI is closed");
        if (!started.compareAndSet(false, true)) return;
        try {
            notifications = source.onAdvance(this::signal);
            // 即使提交后通知丢失，周期检查仍从同一日志游标恢复，不依赖内存信号可靠投递。
            control.scheduleWithFixedDelay(this::tick, 0, settings.pollMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception failed) {
            close();
            throw failed;
        }
    }

    /** 可直接从 GAME 提交完成通知调用；只合并调度信号，不读库、不等待索引或网络。 */
    public void signal() {
        if (!started.get() || closed.get() || !signalQueued.compareAndSet(false, true)) return;
        try { control.execute(() -> { signalQueued.set(false); tick(); }); }
        catch (RejectedExecutionException stopped) { signalQueued.set(false); }
    }

    public CompletableFuture<View> query(Viewport viewport) {
        Objects.requireNonNull(viewport, "viewport");
        long queuedAt = System.nanoTime();
        var result = new CompletableFuture<View>();
        track(result);
        if (closed.get()) { result.completeExceptionally(new Unavailable("AOI stopped")); return result; }
        try {
            queries.execute(() -> {
                queryWait.record(System.nanoTime() - queuedAt, !result.isCancelled());
                if (result.isCancelled()) return;
                long began = System.nanoTime();
                try {
                    AoiIndex current = readableIndex();
                    View view = current.query(viewport);
                    if (current != index || !state.equals("READY")) throw new Unavailable("AOI view is rebuilding");
                    queryExecution.record(System.nanoTime() - began, true);
                    result.complete(view);
                } catch (Throwable error) {
                    queryExecution.record(System.nanoTime() - began, false);
                    result.completeExceptionally(error);
                }
            });
        } catch (RejectedExecutionException full) {
            queryRejected.incrementAndGet();
            result.completeExceptionally(new Overloaded("AOI query budget exhausted"));
        }
        return result;
    }

    /**
     * 取初始视图与登记订阅在同一串行通道完成。首次快照先入有界 outbox，之后的增量只能排在它后面，
     * 因而不存在“查完快照、登记前”漏推窗口。取消在登记前发生时不会遗留订阅。
     */
    public CompletableFuture<Subscription> subscribe(Viewport viewport, ViewUpdateSink sink) {
        Objects.requireNonNull(viewport, "viewport");
        Objects.requireNonNull(sink, "sink");
        var result = new CompletableFuture<Subscription>();
        track(result);
        if (closed.get() || !externalControls.tryAcquire()) {
            result.completeExceptionally(new Overloaded("AOI subscription queue exhausted"));
            return result;
        }
        try { control.execute(() -> {
            try {
                pruneClosed();
                if (result.isCancelled()) return;
                AoiIndex current = readableIndex();
                if (subscribers.size() >= settings.maxSubscriptions()) throw new Overloaded("Too many map subscriptions");
                View initial = current.query(viewport);
                if ((long) subscribedEntries + initial.routes().size() + initial.objects().size() > settings.maxSubscriptionEntries())
                    throw new Overloaded("Map subscription reference budget exhausted");
                Subscriber sub = new Subscriber(++nextSubscription, viewport, sink, initial);
                subscribers.put(sub.id, sub);
                subscriberCount = subscribers.size();
                subscribedEntries += sub.visibleCount();
                if (!sub.enqueue(new Update(identity, initial.generation(), initial.appliedSeq(), initial.appliedSeq(),
                        true, initial.routes(), List.of(), initial.objects(), List.of())))
                    throw new Overloaded("Map output budget exhausted");
                if (!result.complete(sub)) sub.close();
            } catch (Throwable error) { result.completeExceptionally(error); }
            finally { externalControls.release(); }
        }); } catch (RejectedExecutionException stopped) {
            externalControls.release();
            result.completeExceptionally(new Unavailable("AOI stopped"));
        }
        return result;
    }

    private AoiIndex readableIndex() {
        AoiIndex current = index;
        if (closed.get() || current == null || !state.equals("READY")) throw new Unavailable("AOI " + state);
        return current;
    }

    private void track(CompletableFuture<?> call) {
        pendingCalls.add(call);
        call.whenComplete((ignored, error) -> pendingCalls.remove(call));
    }

    private void tick() {
        if (closed.get()) return;
        pruneClosed();
        if (reading || fatal) return;
        reading = true;
        boolean snapshot = rebuild || index == null;
        if (snapshot) {
            state = "REBUILDING";
            resetAll("Map view is rebuilding");
        }
        long after = index == null ? 0 : index.sequence();
        try { reader.execute(() -> read(snapshot, after)); }
        catch (RejectedExecutionException stopped) { reading = false; }
    }

    private void read(boolean snapshot, long after) {
        long began = System.nanoTime();
        Object value = null;
        Throwable failure = null;
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(settings.readTimeoutMillis());
        try {
            Check check = () -> {
                if (closed.get() || Thread.currentThread().isInterrupted()) throw new InterruptedException("AOI read cancelled");
                if (System.nanoTime() >= deadline) throw new IOException("AOI snapshot read timed out");
            };
            check.run();
            if (!identity.equals(source.identity())) throw new BadData("Map source identity changed");
            if (snapshot) {
                AoiIndex staging = new AoiIndex(identity, UUID.randomUUID().toString(), settings);
                long high = source.snapshot(check, staging::addSnapshot, staging::addSnapshot);
                check.run();
                staging.finishSnapshot(high);
                value = staging;
            } else {
                value = source.changes(after, settings.pageSize());
                check.run();
            }
        } catch (Throwable error) { failure = error; }
        // 重建包括固定快照读取和离线索引构建；增量读取不含随后控制线程发布。
        // 超时/坏页也记录，避免只统计追平时的空页而隐藏实际恢复成本。
        (snapshot ? snapshotBuild : deltaRead).record(System.nanoTime() - began, failure == null);
        Object complete = value;
        Throwable error = failure;
        if (!closed.get()) try { control.execute(() -> completeRead(snapshot, complete, error)); }
        catch (RejectedExecutionException stopped) { /* close 已停止控制通道，不再发布半成品。 */ }
    }

    private void completeRead(boolean snapshot, Object value, Throwable error) {
        reading = false;
        if (closed.get()) return;
        long began = System.nanoTime();
        boolean published = false;
        try {
            if (error != null) throw error;
            if (snapshot) {
                AoiIndex replacement = (AoiIndex) value;
                if (index != null && replacement.sequence() < index.sequence())
                    throw new BadData("Snapshot regressed within the same dataset");
                index = replacement;
                sourceHigh = replacement.sequence();
                rebuild = false;
                rebuilds.incrementAndGet();
            } else {
                Page page = (Page) value;
                if (!identity.equals(page.identity())) throw new BadData("Map source identity changed");
                if (page.afterSeq() != index.sequence() || page.prunedThrough() < 0
                        || page.highSeq() < page.prunedThrough() || page.throughSeq() < page.afterSeq()
                        || page.throughSeq() > page.highSeq())
                    throw new BadData("Invalid map page watermarks");
                if (page.prunedThrough() > page.afterSeq()) throw new CursorExpired(page.prunedThrough());
                long before = index.sequence();
                AoiIndex.Applied changed = index.apply(page);
                sourceHigh = page.highSeq();
                publishChanges(before, index.sequence(), changed);
            }
            state = "READY";
            published = true;
            failure = "";
            // 追赶期间连续翻页，每轮仍让已排队的订阅命令执行；追平后由通知或周期检查唤醒。
            if (snapshot || index.sequence() < sourceHigh) signal();
        } catch (CursorExpired | AoiIndex.RebuildRequired expired) {
            rebuild = true;
            state = "REBUILDING";
            failure = "Map replay window expired";
            resetAll(failure);
            signal();
        } catch (Throwable failed) {
            failure = failed.getMessage() == null ? failed.getClass().getSimpleName() : failed.getMessage();
            fatal = failed instanceof BadData || failed instanceof RuntimeException || failed instanceof Error;
            state = fatal ? "FAILED" : "RECOVERING";
            resetAll(failure);
            LOG.log(fatal ? System.Logger.Level.ERROR : System.Logger.Level.WARNING, "AOI source stopped at current cursor: " + failure);
            // 普通读取失败由下次周期检查续读。坏数据必须停留，不能反复重建来掩盖缺口。
        } finally {
            // 包括索引应用与向订阅有界队列扇出，不代表 Gateway 或 Unity 已收到/应用。
            publish.record(System.nanoTime() - began, published);
        }
    }

    private void publishChanges(long before, long through, AoiIndex.Applied changes) {
        if (before == through) return;
        for (Subscriber sub : subscribers.values()) {
            if (sub.cancelled.get()) continue;
            List<Route> upserts = new ArrayList<>();
            List<String> removed = new ArrayList<>();
            List<WorldObject> objectUpserts = new ArrayList<>();
            List<String> removedObjects = new ArrayList<>();
            int added = 0;
            for (Change change : changes.routes()) {
                Route route = change.route();
                boolean visible = route != null && RouteIndex.intersects(new Point(route.fromX(), route.fromY()),
                        new Point(route.targetX(), route.targetY()), sub.viewport);
                if (visible) {
                    if (!sub.visible.containsKey(change.marchId())) added++;
                    upserts.add(route);
                } else if (sub.visible.containsKey(change.marchId())) {
                    removed.add(change.marchId());
                }
            }
            for (ObjectChange change : changes.objects()) {
                WorldObject object = change.object();
                if (PointIndex.visible(object, sub.viewport)) {
                    if (!sub.visibleObjects.containsKey(change.objectId())) added++;
                    objectUpserts.add(object);
                } else if (sub.visibleObjects.containsKey(change.objectId())) removedObjects.add(change.objectId());
            }
            // 按整页最终引用数检查；先看到新增、后看到移除也不能误判为超限。任何超限都终止
            // 整个订阅，而不是先输出行军删除，再静默丢弃同一次决定的矿点占用或城市变化。
            int difference = added - removed.size() - removedObjects.size();
            if ((long) sub.visibleCount() + difference > settings.maxResults()
                    || (long) subscribedEntries + difference > settings.maxSubscriptionEntries()) {
                sub.fail("Map subscription reference budget exceeded");
                continue;
            }
            removed.forEach(sub.visible::remove);
            removedObjects.forEach(sub.visibleObjects::remove);
            upserts.forEach(route -> sub.visible.put(route.marchId(), route));
            objectUpserts.forEach(object -> sub.visibleObjects.put(object.objectId(), object));
            subscribedEntries += difference;
            if (!sub.cancelled.get()) {
                // 无地图变化的日志不必发空包；下一个有效增量从该订阅最后发送位置连续跨到最新 through。
                if (!upserts.isEmpty() || !removed.isEmpty() || !objectUpserts.isEmpty() || !removedObjects.isEmpty()) {
                    sub.enqueue(new Update(identity, index.generation, sub.sentThrough, through, false, upserts, removed,
                            objectUpserts, removedObjects));
                    sub.sentThrough = through;
                }
            }
        }
        pruneClosed();
    }

    private void resetAll(String reason) {
        for (Subscriber sub : subscribers.values()) sub.fail(reason);
        pruneClosed();
    }

    private void pruneClosed() {
        Iterator<Subscriber> iterator = subscribers.values().iterator();
        while (iterator.hasNext()) {
            Subscriber sub = iterator.next();
            if (sub.cancelled.get()) {
                subscribedEntries -= sub.visibleCount();
                iterator.remove();
            }
        }
        subscriberCount = subscribers.size();
    }

    /** 查询健康不触碰权威库。sourceHigh 是最近一次读取观察到的水位，不冒充实时 GAME 水位。 */
    public Map<String, Object> metrics() {
        AoiIndex current = index;
        long applied = current == null ? 0 : current.sequence();
        return Map.ofEntries(Map.entry("state", state), Map.entry("appliedSeq", applied),
                Map.entry("sourceHighSeq", sourceHigh), Map.entry("lag", Math.max(0, sourceHigh - applied)),
                Map.entry("generation", current == null ? "" : current.generation),
                Map.entry("routes", current == null ? 0 : current.size()), Map.entry("subscriptions", subscriberCount),
                Map.entry("objects", current == null ? 0 : current.objectCount()),
                Map.entry("cities", current == null ? 0 : current.cityCount()),
                Map.entry("resourceSites", current == null ? 0 : current.resourceSiteCount()),
                Map.entry("subscriptionEntries", subscribedEntries),
                Map.entry("pendingUpdates", settings.maxPendingUpdates() - pendingUpdates.availablePermits()),
                Map.entry("queryRejected", queryRejected.get()), Map.entry("subscriptionResets", resets.get()),
                Map.entry("queryWait", queryWait.snapshot()), Map.entry("queryExecution", queryExecution.snapshot()),
                Map.entry("snapshotBuild", snapshotBuild.snapshot()), Map.entry("deltaRead", deltaRead.snapshot()),
                Map.entry("publish", publish.snapshot()),
                Map.entry("rebuilds", rebuilds.get()), Map.entry("failure", failure));
    }

    private final class Subscriber implements Subscription {
        final long id;
        final Viewport viewport;
        final ViewUpdateSink sink;
        final Map<String, Route> visible = new HashMap<>(); // 只由 control 修改
        final Map<String, WorldObject> visibleObjects = new HashMap<>();
        final ArrayDeque<Update> outbox = new ArrayDeque<>();
        final AtomicBoolean cancelled = new AtomicBoolean();
        boolean delivering, resetDelivered;
        String resetReason;
        long sentThrough;

        Subscriber(long id, Viewport viewport, ViewUpdateSink sink, View view) {
            this.id = id; this.viewport = viewport; this.sink = sink; sentThrough = view.appliedSeq();
            for (Route route : view.routes()) visible.put(route.marchId(), route);
            for (WorldObject object : view.objects()) visibleObjects.put(object.objectId(), object);
        }

        int visibleCount() { return visible.size() + visibleObjects.size(); }

        synchronized boolean enqueue(Update update) {
            if (cancelled.get()) return false;
            if (outbox.size() >= settings.subscriptionQueueCapacity() || !pendingUpdates.tryAcquire()) {
                fail("Map consumer is too slow; resubscribe for a fresh snapshot");
                return false;
            }
            outbox.addLast(update);
            scheduleDelivery();
            return !cancelled.get();
        }

        private synchronized void scheduleDelivery() {
            if (delivering) return;
            delivering = true;
            try { deliveries.execute(this::drain); }
            catch (RejectedExecutionException full) {
                delivering = false;
                // 线程预算已经满时明确结束订阅。绝不能在 Subscriber 锁内调用外部 Sink：
                // 网络取消可能先持有 Sink 锁再 close()，反向调用会形成 ABBA 死锁。
                cancelled.set(true);
                releaseQueued();
                if (!resetDelivered) {
                    resetDelivered = true;
                    resets.incrementAndGet();
                    try { control.execute(() -> {
                        try { sink.resetRequired("AOI output executor is full"); }
                        catch (RuntimeException ignored) { }
                    }); } catch (RejectedExecutionException stopped) { /* 关停时网络适配器负责结束在途流。 */ }
                }
            }
        }

        private void drain() {
            for (;;) {
                Update update;
                String reset;
                synchronized (this) {
                    reset = !resetDelivered ? resetReason : null;
                    if (reset != null) resetDelivered = true;
                    update = cancelled.get() ? null : outbox.pollFirst();
                    if (update == null && reset == null) { delivering = false; return; }
                }
                if (reset != null) {
                    try { sink.resetRequired(reset); } catch (RuntimeException ignored) { }
                    continue;
                }
                boolean accepted = false;
                try { if (!cancelled.get()) accepted = sink.offer(update); }
                catch (RuntimeException ignored) { }
                finally { pendingUpdates.release(); }
                if (!accepted) fail("Map consumer rejected update; resubscribe for a fresh snapshot");
            }
        }

        synchronized void fail(String reason) {
            if (cancelled.compareAndSet(false, true)) {
                resets.incrementAndGet();
                resetReason = reason;
                releaseQueued();
                scheduleDelivery();
                signal();
            }
        }

        private void releaseQueued() {
            pendingUpdates.release(outbox.size());
            outbox.clear();
        }

        @Override public synchronized void close() {
            if (cancelled.compareAndSet(false, true)) { releaseQueued(); signal(); }
        }
    }

    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        state = "CLOSED";
        for (CompletableFuture<?> call : pendingCalls) call.completeExceptionally(new Unavailable("AOI stopped"));
        if (notifications != null) try { notifications.close(); }
        catch (Exception error) { LOG.log(System.Logger.Level.WARNING, "Cannot remove AOI commit observer", error); }
        // 先停止来源与控制通道，避免释放数据源以后仍有快照读取。所有任务响应中断，队列容量固定。
        reader.shutdownNow();
        control.shutdownNow();
        queries.shutdownNow();
        deliveries.shutdownNow();
        boolean interrupted = false;
        // 不能“等五秒就当作退出”：外层随后会关闭数据库连接池。如果快照仍在读，提前返回会与连接关闭竞争。
        // 本地源按批检查取消；运行时要求所有源和输出适配器遵守取消/非阻塞契约。关停方必须等待真实退出，
        // 即使自身收到中断也先完成资源隔离，最后再恢复中断标记。
        for (var executor : List.of(reader, control, queries, deliveries)) {
            while (!executor.isTerminated()) {
                try { executor.awaitTermination(100, TimeUnit.MILLISECONDS); }
                catch (InterruptedException stop) { interrupted = true; }
            }
        }
        // 控制通道停止后才清理其拥有的订阅；网络适配器通常已经先关闭所有 RPC 流。
        for (Subscriber sub : subscribers.values()) sub.close();
        subscribers.clear();
        subscriberCount = 0;
        subscribedEntries = 0;
        if (interrupted) Thread.currentThread().interrupt();
    }
}
