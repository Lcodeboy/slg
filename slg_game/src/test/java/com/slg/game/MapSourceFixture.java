package com.slg.game;

import com.slg.aoi.MapSnapshotSource;
import com.slg.game.aoi.StructuredMapProjection;
import java.util.*;

/**
 * 有界的公开地图端口夹具：一次 advance 对应一个已提交决定，快照复制后才调用网络回调。
 * 只证明消费者的水位/原子可见性与协议，不模拟 SQL、业务扣兵或真实数据库恢复。
 */
final class MapSourceFixture implements MapSnapshotSource {
    private final Identity identity;
    private final Map<String, Route> routes = new TreeMap<>();
    private final Map<String, WorldObject> objects = new TreeMap<>();
    private final List<Page> events = new ArrayList<>();
    private long high;

    MapSourceFixture(int realm, UUID dataset) {
        identity = new Identity(realm, dataset.toString());
        StructuredMapProjection.staticSites().forEach(site -> objects.put(site.objectId(), site));
    }
    @Override public Identity identity() { return identity; }
    synchronized long high() { return high; }
    synchronized boolean allows(String account) { return objects.containsKey("city:" + identity.realmId() + "_" + account); }

    synchronized long advance(List<Route> upserts, List<String> removed, List<WorldObject> points) {
        if (events.size() >= 128 || upserts.size() + removed.size() + points.size() > 1024
                || routes.size() + objects.size() + upserts.size() + points.size() > 8192)
            throw new AssertionError("Map fixture capacity exceeded");
        long sequence = high + 1;
        var changes = new ArrayList<Change>(); var objectChanges = new ArrayList<ObjectChange>();
        for (var route : upserts) {
            routes.put(route.marchId(), route);
            changes.add(new Change(sequence, route.marchId(), route.version(), route));
        }
        for (var id : removed) {
            var before = Objects.requireNonNull(routes.remove(id), "Missing fixture route");
            changes.add(new Change(sequence, id, before.version() + 1, null));
        }
        for (var object : points) {
            objects.put(object.objectId(), object);
            objectChanges.add(new ObjectChange(sequence, object.objectId(), object.version(), object));
        }
        events.add(new Page(identity, high, 0, sequence, sequence, changes, objectChanges));
        return high = sequence;
    }

    @Override public long snapshot(Check check, SnapshotReceiver receiver) throws Exception {
        final long view; final List<Route> routeView; final List<WorldObject> objectView;
        synchronized (this) { view = high; routeView = List.copyOf(routes.values()); objectView = List.copyOf(objects.values()); }
        check.run(); receiver.begin(view); check.run();
        for (var route : routeView) { check.run(); receiver.route(route); }
        for (var object : objectView) { check.run(); receiver.object(object); }
        check.run(); return view;
    }
    @Override public synchronized Page changes(long after, int limit) throws Exception {
        if (after > high) throw new CursorAhead(high);
        if (after < 0 || limit < 1 || limit > 1024) throw new IllegalArgumentException("Invalid fixture page");
        long through = Math.min(high, after + limit);
        var changes = new ArrayList<Change>(); var points = new ArrayList<ObjectChange>();
        for (long cursor = after; cursor < through; cursor++) {
            var event = events.get(Math.toIntExact(cursor)); changes.addAll(event.changes()); points.addAll(event.objectChanges());
        }
        return new Page(identity, after, 0, high, through, changes, points);
    }
}
