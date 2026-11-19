package com.slg.aoi;

import com.slg.aoi.MapStateSource.WorldObject;
import com.slg.core.world.RouteIndex.QueryBudget;
import com.slg.core.world.Viewport;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 城市和矿点的分块倒排索引。只遍历视野覆盖的分块，不因大区有十万城市就扫描全表。
 * 每个点只属于一个块；块边界按 floor 归属，查询边界包含对应块，精确过滤使用闭矩形。
 * 所有读写均由外层 AoiIndex 的同一把锁保护，才能与路线及区服水位一起原子发布。
 */
final class PointIndex {
    private final int mapSize, chunkSize;
    private final Map<String, WorldObject> objects = new HashMap<>();
    private final Map<Long, Set<String>> chunks = new HashMap<>();

    PointIndex(int mapSize, int chunkSize) {
        this.mapSize = mapSize;
        this.chunkSize = chunkSize;
    }

    void upsert(WorldObject object) {
        WorldObject old = objects.put(object.objectId(), object);
        if (old != null) removeChunk(old);
        chunks.computeIfAbsent(chunk(object), ignored -> new HashSet<>()).add(object.objectId());
    }

    void remove(String id) {
        WorldObject old = objects.remove(id);
        if (old != null) removeChunk(old);
    }

    private void removeChunk(WorldObject object) {
        long key = chunk(object);
        Set<String> ids = chunks.get(key);
        ids.remove(object.objectId());
        if (ids.isEmpty()) chunks.remove(key);
    }

    List<WorldObject> query(Viewport viewport, QueryBudget budget) {
        if (viewport.maxX() < 0 || viewport.maxY() < 0 || viewport.minX() > mapSize - 1d
                || viewport.minY() > mapSize - 1d) return List.of();
        int minX = (int) Math.max(0, viewport.minX()) / chunkSize;
        int minY = (int) Math.max(0, viewport.minY()) / chunkSize;
        int maxX = (int) Math.min(mapSize - 1d, viewport.maxX()) / chunkSize;
        int maxY = (int) Math.min(mapSize - 1d, viewport.maxY()) / chunkSize;
        var visible = new ArrayList<WorldObject>();
        for (int x = minX; x <= maxX; x++) for (int y = minY; y <= maxY; y++) {
            Set<String> ids = chunks.get(key(x, y));
            if (ids == null) continue;
            for (String id : ids) {
                // 热点城市可能同块，必须在累积结果前消费候选预算，不能先把整个块复制出来。
                budget.candidate();
                WorldObject object = objects.get(id);
                if (visible(object, viewport)) { budget.result(); visible.add(object); }
            }
        }
        visible.sort(Comparator.comparing(WorldObject::objectId));
        return List.copyOf(visible);
    }

    static boolean visible(WorldObject object, Viewport viewport) {
        return object != null && object.x() >= viewport.minX() && object.x() <= viewport.maxX()
                && object.y() >= viewport.minY() && object.y() <= viewport.maxY();
    }

    private long chunk(WorldObject object) { return key(object.x() / chunkSize, object.y() / chunkSize); }
    private static long key(int x, int y) { return ((long) x << 32) | (y & 0xffffffffL); }
}
