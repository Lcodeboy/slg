package com.slg.aoi;

import com.slg.aoi.MapStateSource.*;
import com.slg.core.world.MarchRoute;
import com.slg.core.world.Point;
import com.slg.core.world.RouteIndex;
import com.slg.core.world.Viewport;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 一份区服视图。只有 AOI 更新通道写入；查询在同一读锁下取得几何、公开字段和游标的完整快照。
 * 锁从不与 GAME 对象资格或数据库事务锁嵌套，因此热点查询只会影响 AOI 自身。
 */
final class AoiIndex {
    final Identity identity;
    final String generation;
    private final AoiRuntime.Settings limits;
    private final RouteIndex geometry;
    private final PointIndex points;
    private final Map<String, Route> routes = new HashMap<>();
    private final Map<String, Long> versions = new HashMap<>();
    private final Map<String, WorldObject> objects = new HashMap<>();
    private final Map<String, Long> objectVersions = new HashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private long sequence;
    private int cities, resourceSites;

    /** 同一页两类投影的最终后像；索引先完整发布，订阅再用同一 throughSeq 输出这些变化。 */
    record Applied(List<Change> routes, List<ObjectChange> objects) {
        boolean isEmpty() { return routes.isEmpty() && objects.isEmpty(); }
    }

    AoiIndex(Identity identity, String generation, AoiRuntime.Settings limits) {
        this.identity = identity;
        this.generation = generation;
        this.limits = limits;
        geometry = new RouteIndex(limits.mapSize(), limits.chunkSize());
        points = new PointIndex(limits.mapSize(), limits.chunkSize());
    }

    /** 快照构建器只在读取线程私有使用，尚未发布时无需让查询看到任何中间状态。 */
    void addSnapshot(Route route) {
        validate(route);
        if (routes.containsKey(route.marchId())) throw new IllegalArgumentException("Duplicate route in snapshot");
        if (routes.size() == limits.maxRoutes()) throw new CapacityExceeded("Map route budget exceeded");
        snapshotVersionBudget();
        routes.put(route.marchId(), route);
        versions.put(route.marchId(), route.version());
        geometry.upsert(geometry(route));
    }

    /** 静态矿点和动态矿点必须先在来源合并；重复 ID 表示快照错误，不能取最后一条掩盖水位混用。 */
    void addSnapshot(WorldObject object) {
        validate(object);
        if (objects.containsKey(object.objectId())) throw new IllegalArgumentException("Duplicate object in snapshot");
        if (objects.size() == limits.maxObjects()) throw new CapacityExceeded("Map object budget exceeded");
        snapshotVersionBudget();
        objects.put(object.objectId(), object);
        objectVersions.put(object.objectId(), object.version());
        points.upsert(object);
        count(object, 1);
    }

    private void snapshotVersionBudget() {
        if ((long) versions.size() + objectVersions.size() >= limits.maxTrackedVersions())
            throw new CapacityExceeded("Map snapshot version budget exceeded");
    }

    void finishSnapshot(long high) {
        if (high < 0) throw new IllegalArgumentException("Negative snapshot sequence");
        for (Route route : routes.values()) if (route.version() > high)
            throw new IllegalArgumentException("Route version is ahead of snapshot sequence");
        for (WorldObject object : objects.values()) if (object.version() > high)
            throw new IllegalArgumentException("Object version is ahead of snapshot sequence");
        sequence = high;
    }

    /**
     * 先验证整页再修改。跨分块改道、删除和 throughSeq 在同一写锁内发布，查询无法看到半页。
     * 已应用的整页可忽略；部分重叠或跳过起点不能靠“取最大序号”掩盖。
     */
    Applied apply(Page page) throws MapStateSource.BadData {
        lock.writeLock().lock();
        try {
            if (!identity.equals(page.identity())) throw new MapStateSource.BadData("Map source identity changed");
            if (page.afterSeq() < 0 || page.prunedThrough() < 0 || page.highSeq() < page.prunedThrough()
                    || page.throughSeq() < page.afterSeq() || page.throughSeq() > page.highSeq())
                throw new MapStateSource.BadData("Invalid map page watermarks");
            if (page.afterSeq() < sequence && page.throughSeq() <= sequence) return new Applied(List.of(), List.of());
            if (page.afterSeq() != sequence || page.prunedThrough() > sequence)
                throw new MapStateSource.BadData("Map page does not continue applied cursor");
            if (page.throughSeq() == sequence && page.highSeq() != sequence)
                throw new MapStateSource.BadData("Map page made no progress despite pending journal");

            Map<String, Change> latest = new LinkedHashMap<>();
            long previous = page.afterSeq();
            int nextSize = routes.size();
            int newVersions = 0;
            for (Change change : page.changes()) {
                if (change.sequence() <= page.afterSeq() || change.sequence() < previous
                        || change.sequence() > page.throughSeq() || change.version() > change.sequence())
                    throw new MapStateSource.BadData("Out-of-order map changes");
                previous = change.sequence();
                if (change.route() != null) validate(change.route());
                Change pending = latest.get(change.marchId());
                long oldVersion = pending == null ? versions.getOrDefault(change.marchId(), 0L) : pending.version();
                Route oldRoute = pending == null ? routes.get(change.marchId()) : pending.route();
                if (change.version() <= oldVersion) {
                    if (change.version() == oldVersion && java.util.Objects.equals(oldRoute, change.route())) continue;
                    throw new MapStateSource.BadData("Route version regressed or conflicts");
                }
                if (pending == null && !versions.containsKey(change.marchId())) newVersions++;
                if (oldRoute == null && change.route() != null) nextSize++;
                if (oldRoute != null && change.route() == null) nextSize--;
                latest.put(change.marchId(), change);
            }
            if (nextSize > limits.maxRoutes()) throw new CapacityExceeded("Map route budget exceeded");

            Map<String, ObjectChange> latestObjects = new LinkedHashMap<>();
            previous = page.afterSeq();
            int nextObjects = objects.size();
            for (ObjectChange change : page.objectChanges()) {
                if (change.sequence() <= page.afterSeq() || change.sequence() < previous
                        || change.sequence() > page.throughSeq() || change.version() > change.sequence())
                    throw new MapStateSource.BadData("Out-of-order map object changes");
                previous = change.sequence();
                if (change.object() != null) validate(change.object());
                ObjectChange pending = latestObjects.get(change.objectId());
                long oldVersion = pending == null ? objectVersions.getOrDefault(change.objectId(), 0L) : pending.version();
                WorldObject old = pending == null ? objects.get(change.objectId()) : pending.object();
                if (change.version() <= oldVersion) {
                    if (change.version() == oldVersion && java.util.Objects.equals(old, change.object())) continue;
                    throw new MapStateSource.BadData("Map object version regressed or conflicts");
                }
                if (pending == null && !objectVersions.containsKey(change.objectId())) newVersions++;
                if (old == null && change.object() != null) nextObjects++;
                if (old != null && change.object() == null) nextObjects--;
                latestObjects.put(change.objectId(), change);
            }
            if (nextObjects > limits.maxObjects()) throw new CapacityExceeded("Map object budget exceeded");
            // 路线与点对象共用墓碑预算。上面的两类验证全部完成后才动任何索引；否则坏矿点页会
            // 先删掉行军线，再失败于占矿记录，向查询暴露“军队消失但矿仍空闲”的半个决定。
            if ((long) versions.size() + objectVersions.size() + newVersions > limits.maxTrackedVersions())
                throw new RebuildRequired();
            for (Change change : latest.values()) {
                if (change.route() == null) {
                    geometry.remove(change.marchId());
                    routes.remove(change.marchId());
                } else {
                    geometry.upsert(geometry(change.route()));
                    routes.put(change.marchId(), change.route());
                }
                versions.put(change.marchId(), change.version());
            }
            for (ObjectChange change : latestObjects.values()) {
                WorldObject old = objects.remove(change.objectId());
                if (old != null) count(old, -1);
                if (change.object() == null) points.remove(change.objectId());
                else {
                    objects.put(change.objectId(), change.object());
                    points.upsert(change.object());
                    count(change.object(), 1);
                }
                objectVersions.put(change.objectId(), change.version());
            }
            sequence = page.throughSeq();
            return new Applied(List.copyOf(latest.values()), List.copyOf(latestObjects.values()));
        } catch (IllegalArgumentException invalid) {
            throw new MapStateSource.BadData("Invalid object or route in map page", invalid);
        } finally { lock.writeLock().unlock(); }
    }

    AoiRuntime.View query(Viewport viewport) {
        lock.readLock().lock();
        try {
            var visible = new ArrayList<Route>();
            var budget = new RouteIndex.QueryBudget(limits.maxCandidates(), limits.maxResults());
            for (MarchRoute route : geometry.query(viewport, budget))
                visible.add(routes.get(route.marchId()));
            // 两个索引共享查询预算和读锁。超出任一上限时丢弃整个结果，不返回只含路线的半幅地图。
            return new AoiRuntime.View(identity, generation, sequence, visible, points.query(viewport, budget));
        } finally { lock.readLock().unlock(); }
    }

    long sequence() {
        lock.readLock().lock();
        try { return sequence; } finally { lock.readLock().unlock(); }
    }

    int size() {
        lock.readLock().lock();
        try { return routes.size(); } finally { lock.readLock().unlock(); }
    }

    int objectCount() {
        lock.readLock().lock();
        try { return objects.size(); } finally { lock.readLock().unlock(); }
    }

    int cityCount() {
        lock.readLock().lock();
        try { return cities; } finally { lock.readLock().unlock(); }
    }

    int resourceSiteCount() {
        lock.readLock().lock();
        try { return resourceSites; } finally { lock.readLock().unlock(); }
    }

    private void count(WorldObject object, int direction) {
        if (object instanceof City) cities += direction;
        else resourceSites += direction;
    }

    private void validate(WorldObject object) {
        if (object.x() >= limits.mapSize() || object.y() >= limits.mapSize())
            throw new IllegalArgumentException("Map object outside map bounds");
    }

    private void validate(Route route) {
        if (route.fromX() > limits.mapSize() - 1d || route.fromY() > limits.mapSize() - 1d
                || route.targetX() >= limits.mapSize() || route.targetY() >= limits.mapSize())
            throw new IllegalArgumentException("Route outside map bounds");
    }

    static MarchRoute geometry(Route route) {
        return new MarchRoute(route.marchId(), new Point(route.fromX(), route.fromY()),
                new Point(route.targetX(), route.targetY()), route.departAt(), route.arriveAt(), route.version());
    }

    static final class RebuildRequired extends RuntimeException { }
    static final class CapacityExceeded extends RuntimeException {
        CapacityExceeded(String message) { super(message); }
    }
}
