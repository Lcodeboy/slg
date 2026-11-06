package com.slg.core.world;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 线程安全的内存路线索引，查询整条路线是否经过视野，与部队当前位置的兴趣区域查询相互独立。
 * 地图坐标范围为 [0, mapSize - 1]，几何相交按闭边界并使用 1e-9 的浮点容差。
 * 路线保守登记到所有接触的分块，包括只接触边角的分块；查询先去重候选，再做线段与矩形相交判断。
 * 写操作由权威世界状态的拥有者调用。本类不是持久化存储，不提供删除墓碑或事件重放去重。
 */
public final class RouteIndex {
    /** 几何比较容差，单位为地图格，防止共享边界因浮点舍入漏检。 */
    private static final double EPSILON = 1e-9;
    /** 地图每个轴上的整数格点数，例如 1200 对应坐标 0 到 1199。 */
    private final int mapSize;
    /** 每个分块的边长，单位为地图格。 */
    private final int chunkSize;
    /** 单轴分块数量，末尾允许存在不足完整边长的分块。 */
    private final int chunkCount;
    /** 行军标识到路线及其覆盖分块的正向索引。 */
    private final Map<String, IndexedRoute> routes = new HashMap<>();
    /** 分块到行军标识集合的倒排索引，避免局部视野查询扫描全部路线。 */
    private final Map<Long, Set<String>> chunks = new HashMap<>();
    /** 将正向索引与倒排索引作为一个整体保护，查询不会看到改道的中间状态。 */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * 创建指定地图与分块尺寸的空索引。
     *
     * @param mapSize 单轴整数格点数，至少为 2
     * @param chunkSize 分块边长，至少为 1 格
     */
    public RouteIndex(int mapSize, int chunkSize) {
        if (mapSize < 2 || chunkSize < 1) {
            throw new IllegalArgumentException("Map size must be at least 2 and chunk size positive");
        }
        this.mapSize = mapSize;
        this.chunkSize = chunkSize;
        this.chunkCount = (int) (((long) mapSize + chunkSize - 1) / chunkSize);
    }

    /**
     * 原子新增或替换一条路线，并在改道时清理旧分块引用。
     * 已有路线的完全相同快照可幂等重投；内容不同的同版本或旧版本更新会被拒绝。
     *
     * @param route 位于当前地图内的路线快照
     * @throws IllegalArgumentException 端点越界，或更新版本没有递增
     */
    public void upsert(MarchRoute route) {
        Objects.requireNonNull(route, "route");
        validatePoint(route.from());
        validatePoint(route.to());
        Set<Long> covered = coveredChunks(route);
        lock.writeLock().lock();
        try {
            IndexedRoute old = routes.get(route.marchId());
            if (old != null && route.version() <= old.route().version()) {
                if (route.equals(old.route())) return;
                throw new IllegalArgumentException("Route update must have a newer version");
            }
            if (old != null) removeIndex(old);
            routes.put(route.marchId(), new IndexedRoute(route, covered));
            for (long chunk : covered) {
                chunks.computeIfAbsent(chunk, ignored -> new HashSet<>()).add(route.marchId());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 原子移除路线及其全部分块引用。删除后不保留版本墓碑，调用方须防止旧事件重新插入。
     *
     * @param marchId 待移除的行军标识
     * @return 原索引存在该路线时为 true，重复删除返回 false
     */
    public boolean remove(String marchId) {
        Objects.requireNonNull(marchId, "marchId");
        lock.writeLock().lock();
        try {
            IndexedRoute removed = routes.remove(marchId);
            if (removed == null) return false;
            removeIndex(removed);
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 返回整条线段与视野相交的路线快照，端点和部队当前位置均不必位于视野内。
     * 只访问视野覆盖分块，跨多个分块的同一行军只返回一次；结果不可变且按行军标识排序。
     * 玩家权限、战争迷雾及敌我可见性由调用方过滤。
     *
     * @param viewport 观察区域，允许部分或全部位于地图外
     * @return 与视野相交的路线，完全位于地图外的视野返回空列表
     */
    public List<MarchRoute> query(Viewport viewport) {
        Objects.requireNonNull(viewport, "viewport");
        if (viewport.maxX() < -EPSILON || viewport.maxY() < -EPSILON
                || viewport.minX() > mapSize - 1.0 + EPSILON
                || viewport.minY() > mapSize - 1.0 + EPSILON) return List.of();
        int minX = firstChunk(Math.max(0, viewport.minX()));
        int maxX = lastChunk(Math.min(mapSize - 1.0, viewport.maxX()));
        int minY = firstChunk(Math.max(0, viewport.minY()));
        int maxY = lastChunk(Math.min(mapSize - 1.0, viewport.maxY()));
        lock.readLock().lock();
        try {
            // 一条长路线会出现在多个分块中，必须先按行军标识去重再执行几何过滤。
            Set<String> candidates = new HashSet<>();
            for (int x = minX; x <= maxX; x++) {
                for (int y = minY; y <= maxY; y++) {
                    Set<String> ids = chunks.get(chunkKey(x, y));
                    if (ids != null) candidates.addAll(ids);
                }
            }
            List<MarchRoute> visible = new ArrayList<>();
            for (String id : candidates) {
                MarchRoute route = routes.get(id).route();
                if (intersects(route.from(), route.to(), viewport)) visible.add(route);
            }
            visible.sort(Comparator.comparing(MarchRoute::marchId));
            return List.copyOf(visible);
        } finally {
            lock.readLock().unlock();
        }
    }

    /** 返回当前路线数量，同一行军跨多个分块仍只计一次。 */
    public int size() {
        lock.readLock().lock();
        try {
            return routes.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /** 校验路线端点位于当前地图闭区间内，不接受仅在浮点容差内的越界写入。 */
    private void validatePoint(Point point) {
        if (point.x() < 0 || point.y() < 0 || point.x() > mapSize - 1.0 || point.y() > mapSize - 1.0) {
            throw new IllegalArgumentException("Route coordinate is outside this map");
        }
    }

    /** 在持有写锁时移除倒排引用，并释放已经没有路线的分块集合。 */
    private void removeIndex(IndexedRoute indexed) {
        for (long key : indexed.chunks()) {
            Set<String> ids = chunks.get(key);
            ids.remove(indexed.route().marchId());
            if (ids.isEmpty()) chunks.remove(key);
        }
    }

    /**
     * 逐列裁剪线段，计算保守覆盖的全部分块，而非枚举线段外包矩形内的所有分块。
     * 每列只遍历线段在该列经过的纵向区间，开销随实际经过的行列增长。
     * 列边界向两侧扩展容差、行边界使用闭区间，使压边路线和穿角路线同时登记到相邻分块。
     *
     * @param route 已通过地图边界校验的路线
     * @return 不可变的分块键集合，后续精确相交判断会排除保守覆盖产生的多余候选
     */
    private Set<Long> coveredChunks(MarchRoute route) {
        Point from = route.from();
        Point to = route.to();
        double dx = to.x() - from.x();
        double dy = to.y() - from.y();
        Set<Long> covered = new HashSet<>();
        int first = firstChunk(Math.min(from.x(), to.x()));
        int last = lastChunk(Math.max(from.x(), to.x()));
        for (int x = first; x <= last; x++) {
            double start = 0;
            double end = 1;
            // 用参数区间 [0, 1] 裁出当前列中的线段；竖直线保持完整参数区间，避免除零。
            if (dx != 0) {
                double a = ((double) x * chunkSize - EPSILON - from.x()) / dx;
                double b = ((double) (x + 1) * chunkSize + EPSILON - from.x()) / dx;
                // min/max 同时兼容由左向右和由右向左的行军方向。
                start = Math.max(0, Math.min(a, b));
                end = Math.min(1, Math.max(a, b));
                if (start > end) continue;
            }
            double y1 = from.y() + dy * start;
            double y2 = from.y() + dy * end;
            int firstY = firstChunk(Math.min(y1, y2));
            int lastY = lastChunk(Math.max(y1, y2));
            for (int y = firstY; y <= lastY; y++) covered.add(chunkKey(x, y));
        }
        return Set.copyOf(covered);
    }

    /** 取坐标接触的最小分块编号，压在公共边界时包含边界左侧或下侧分块。 */
    private int firstChunk(double coordinate) {
        return boundedChunk((int) Math.floor((coordinate - EPSILON) / chunkSize));
    }

    /** 取坐标接触的最大分块编号，压在公共边界时包含边界右侧或上侧分块。 */
    private int lastChunk(double coordinate) {
        return boundedChunk((int) Math.floor((coordinate + EPSILON) / chunkSize));
    }

    /** 将分块编号限制在当前地图的有效范围内。 */
    private int boundedChunk(int coordinate) {
        return Math.max(0, Math.min(chunkCount - 1, coordinate));
    }

    /** 将横纵分块编号分别存入高、低 32 位，生成无冲突的二元坐标键。 */
    private static long chunkKey(int x, int y) {
        return ((long) x << 32) | (y & 0xffffffffL);
    }

    /**
     * 用 Liang-Barsky 参数裁剪判断闭线段与矩形是否相交，支持零长度线段和边角接触。
     * 四个矩形边界分别收缩同一个参数区间；区间非空表示存在公共点。
     *
     * @param from 线段起点
     * @param to 线段终点
     * @param viewport 待判断的闭合矩形
     * @return 两者相交或在 1e-9 地图格容差内接触时为 true
     */
    public static boolean intersects(Point from, Point to, Viewport viewport) {
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
        Objects.requireNonNull(viewport, "viewport");
        double dx = to.x() - from.x();
        double dy = to.y() - from.y();
        double[] interval = {0.0, 1.0};
        return clip(-dx, from.x() - viewport.minX() + EPSILON, interval)
                && clip(dx, viewport.maxX() - from.x() + EPSILON, interval)
                && clip(-dy, from.y() - viewport.minY() + EPSILON, interval)
                && clip(dy, viewport.maxY() - from.y() + EPSILON, interval);
    }

    /**
     * 按一个矩形半平面裁剪线段参数区间，原地更新区间以避免额外对象。
     *
     * @param direction 当前边对应的有向增量，符号决定裁剪进入端还是离开端
     * @param distance 起点到当前边的有向距离，已包含边界容差
     * @param interval 长度为 2 的闭参数区间，依次存放进入值和离开值
     * @return 裁剪后仍有公共区间时为 true
     */
    private static boolean clip(double direction, double distance, double[] interval) {
        // 与边界平行时无法求交点，只需判断整段是否处于允许的半平面。
        if (direction == 0) return distance >= 0;
        double ratio = distance / direction;
        if (direction < 0) {
            if (ratio > interval[1]) return false;
            interval[0] = Math.max(interval[0], ratio);
        } else {
            if (ratio < interval[0]) return false;
            interval[1] = Math.min(interval[1], ratio);
        }
        return true;
    }

    /**
     * 正向索引条目，同时保存反向引用，便于改道或删除时只清理涉及的分块。
     *
     * @param route 不可变路线快照
     * @param chunks 路线保守覆盖的不可变分块键集合
     */
    private record IndexedRoute(MarchRoute route, Set<Long> chunks) { }
}
