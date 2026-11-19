package com.slg.game.aoi;

import com.slg.aoi.MapSnapshotSource;
import com.slg.aoi.MapStateSource;
import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.game.application.port.*;
import com.slg.game.domain.persistence.*;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * 结构化端口→公开投影的边界验证；小型不可变读视图替代 SQL，不证明 JDBC 的 RR 或分页成本。
 * SQL 事务/预算仍由 MySqlStateReadersIT、MySqlEventReadIT 验证，网络消费者另由 AOI/MapFeed 测试验证。
 */
class MySqlMapStateSourceTest {
    private static final RealmIdentity REALM = new RealmIdentity(17, UUID.randomUUID());
    private static final ReadBudget BUDGET = new ReadBudget(100, 100_000, Duration.ofSeconds(3));

    @Test void oneSnapshotKeepsItsWatermarkAndProfileVersionAndOverlaysOnlyPersistedSites() throws Exception {
        var rows = new Rows(3, List.of(row(7, player("Alice", 17)), row(2, march(MarchState.Status.OUTBOUND, 1150)),
                row(4, new GatherSiteState(650, 650, "17_101", "march", 2))));
        var source = source(rows, emptyEvents());
        var routes = new ArrayList<MapStateSource.Route>(); var objects = new ArrayList<MapStateSource.WorldObject>();
        var begins = new ArrayList<Long>();
        long complete = source.snapshot(() -> {}, new MapSnapshotSource.SnapshotReceiver() {
            @Override public void begin(long high) {
                begins.add(high);
                // 后续提交只改变下一次读取；本次路线/城市/矿点必须来自打开时的同一份视图。
                rows.high = 4; rows.rows = List.of(row(8, player("Later", 17)));
            }
            @Override public void route(MapStateSource.Route route) { routes.add(route); }
            @Override public void object(MapStateSource.WorldObject object) { objects.add(object); }
        });
        assertEquals(List.of(3L), begins); assertEquals(3, complete);
        assertEquals(1, rows.opened); assertEquals(1, rows.closed); assertSame(BUDGET, rows.budget);
        assertEquals(List.of(ObjectKind.PLAYER, ObjectKind.GATHER_SITE), rows.scans);
        assertEquals(1, rows.movingScans); assertEquals(1, routes.size()); assertEquals(2, routes.getFirst().version());
        assertEquals(new MapStateSource.City("17_101", 7, 0, 0, "Alice"), objects.getFirst());
        var sites = objects.stream().filter(MapStateSource.ResourceSite.class::isInstance).map(MapStateSource.ResourceSite.class::cast).toList();
        assertEquals(3, sites.size());
        assertEquals(2, sites.stream().filter(site -> site.version() == 0 && site.occupantPlayerId().isEmpty()).count());
        assertTrue(sites.contains(new MapStateSource.ResourceSite(650, 650, 4, "WOOD", "17_101")));
    }

    @Test void privateDecisionStillAdvancesAndOneArrivalKeepsRouteRemovalAndMineTogether() throws Exception {
        var asset = new ResourceState("17_101", ResourceType.WOOD, 500);
        var gathering = march(MarchState.Status.GATHERING, 150);
        var occupied = new GatherSiteState(150, 150, "17_101", "march", 2);
        var release = new GatherSiteState(150, 150, null, null, 0);
        var events = List.of(event(1, change(1, asset)), event(2, change(1, gathering), change(0, occupied)),
                event(3, change(2, march(MarchState.Status.RETURNING, 0)), change(1, release)));
        var source = source(new Rows(3, List.of()), (identity, after, limit, budget) -> {
            assertEquals(REALM, identity); assertSame(BUDGET, budget);
            long through = Math.min(events.size(), after + limit);
            return new RealmEventReader.EventPage(identity, after, 0, 3, through,
                    events.subList(Math.toIntExact(after), Math.toIntExact(through)));
        });
        var privatePage = source.changes(0, 1);
        assertEquals(1, privatePage.throughSeq()); assertTrue(privatePage.changes().isEmpty()); assertTrue(privatePage.objectChanges().isEmpty());
        var arrival = source.changes(1, 1);
        assertEquals(2, arrival.throughSeq()); assertEquals(1, arrival.changes().size()); assertNull(arrival.changes().getFirst().route());
        assertEquals(2, arrival.changes().getFirst().version()); assertEquals(2, arrival.objectChanges().getFirst().sequence());
        assertEquals(new MapStateSource.ResourceSite(150, 150, 1, "WOOD", "17_101"), arrival.objectChanges().getFirst().object());
        var returning = source.changes(2, 1);
        assertEquals(MapStateSource.Direction.RETURNING, returning.changes().getFirst().route().direction());
        assertEquals(new MapStateSource.ResourceSite(150, 150, 2, "WOOD", ""), returning.objectChanges().getFirst().object());
        assertTrue(returning.objectChanges().stream().noneMatch(change -> change.object() instanceof MapStateSource.City),
                "兵力/资源/行军不能伪增城市资料版本");
    }

    @Test void cancellationClosesTheViewAndInvalidIdentityOrCursorNeverBecomesAnEmptySuccess() throws Exception {
        var rows = new Rows(1, List.of(row(1, player("Alice", 17)))); var source = source(rows, emptyEvents());
        var cancelled = new IOException("test callback cancellation");
        assertSame(cancelled, assertThrows(IOException.class, () -> source.snapshot(() -> {}, new MapSnapshotSource.SnapshotReceiver() {
            @Override public void begin(long high) { }
            @Override public void route(MapStateSource.Route route) { }
            @Override public void object(MapStateSource.WorldObject object) throws Exception { throw cancelled; }
        })));
        assertEquals(1, rows.closed);
        var access = new MySqlMapAccess(rows, REALM, BUDGET);
        assertTrue(access.allows("101")); assertFalse(access.allows("102"));
        rows.rows = List.of(row(1, player("Alice", 18)));
        assertEquals(StorageAccessException.Code.INVALID_DATA, assertThrows(StorageAccessException.class, () -> access.allows("101")).code());
        assertThrows(MapStateSource.BadData.class, () -> source.snapshot(() -> {}, ignored -> {}, ignored -> {}));
        assertThrows(MapStateSource.BadData.class, () -> StructuredMapProjection.city(row(1, player(" bad", 17)), 17));
        assertThrows(MapStateSource.BadData.class, () -> StructuredMapProjection.route(row(1, march(MarchState.Status.OUTBOUND, 1200))));
        var expired = source(rows, (identity, after, limit, budget) -> { throw new RealmEventReader.CursorExpired(7); });
        assertEquals(7, assertThrows(MapStateSource.CursorExpired.class, () -> expired.changes(3, 1)).prunedThrough());
        var ahead = source(rows, (identity, after, limit, budget) -> { throw new RealmEventReader.CursorAhead(4); });
        assertEquals(4, assertThrows(MapStateSource.CursorAhead.class, () -> ahead.changes(5, 1)).high());
        rows.failure = new StorageAccessException(StorageAccessException.Code.UNAVAILABLE, "test database offline");
        assertThrows(StorageAccessException.class, () -> access.allows("101"));
    }

    private static MySqlMapStateSource source(Rows rows, RealmEventReader events) { return new MySqlMapStateSource(rows, events, REALM, BUDGET, BUDGET); }
    private static RealmEventReader emptyEvents() { return (identity, after, limit, budget) -> new RealmEventReader.EventPage(identity, after, 0, after, after, List.of()); }
    private static PlayerState player(String name, int currentRealm) { return new PlayerState("17_101", 101, 17, currentRealm, name); }
    private static VersionedState row(long version, ObjectState state) { return new VersionedState(version, state); }
    private static ObjectChange change(long before, ObjectState state) { return new ObjectChange(state.key(), before, state); }
    private static CommittedEvent event(long sequence, ObjectChange... changes) {
        return new CommittedEvent(sequence, new CommitDecision(new RequestKey("fixture", "event-" + sequence), "digest", "FACT", "{}", "{}", List.of(changes), Map.of()));
    }
    private static MarchState march(MarchState.Status status, int target) {
        return new MarchState("march", "17_101", status, MarchState.Mission.GATHER, 0, 0, target, target,
                1000, 4000, 1, Map.of(TroopType.INFANTRY, 100L), List.of(), "task", "rules-v1", Map.of());
    }
    /** 最多16行的端口替身只固定读视图；不实现一份伪数据库或复制SQL预算逻辑。 */
    private static final class Rows implements RealmStateReader {
        long high; List<VersionedState> rows; int opened, closed, movingScans; ReadBudget budget;
        StorageAccessException failure; final List<ObjectKind> scans = new ArrayList<>();
        Rows(long high, List<VersionedState> rows) { this.high = high; this.rows = List.copyOf(rows); }
        @Override public Snapshot openSnapshot(RealmIdentity identity, ReadBudget requested) throws StorageAccessException {
            if (failure != null) throw failure;
            if (rows.size() > 16) throw new AssertionError("Fixture row limit");
            opened++; budget = requested; var view = List.copyOf(rows); long watermark = high;
            return new Snapshot() {
                boolean ended;
                private void active() { if (ended) throw new IllegalStateException("Closed fixture snapshot"); }
                @Override public RealmIdentity identity() { active(); return identity; }
                @Override public long highSequence() { active(); return watermark; }
                @Override public Optional<VersionedState> find(ObjectKey key) { active(); return view.stream().filter(row -> row.state().key().equals(key)).findFirst(); }
                @Override public Optional<VersionedState> findPlayerByAccount(long account) {
                    active(); return view.stream().filter(row -> row.state() instanceof PlayerState player && player.accountId() == account).findFirst();
                }
                @Override public void scan(ObjectKind kind, Consumer<StateEntry> consumer) {
                    active(); scans.add(kind); view.stream().filter(row -> row.state().key().kind() == kind)
                            .forEach(row -> consumer.accept(new StateEntry(row.state().key(), row)));
                }
                @Override public void scanMovingMarches(Consumer<StateEntry> consumer) {
                    active(); movingScans++; view.stream().filter(row -> row.state() instanceof MarchState march
                            && (march.status() == MarchState.Status.OUTBOUND || march.status() == MarchState.Status.RETURNING))
                            .forEach(row -> consumer.accept(new StateEntry(row.state().key(), row)));
                }
                @Override public void scanOwned(ObjectKind kind, String player, Consumer<StateEntry> consumer) { throw new AssertionError("Map must not load player assets"); }
                @Override public void scanActiveMarches(String player, Consumer<StateEntry> consumer) { throw new AssertionError("Map must use moving scan"); }
                @Override public void close() { if (!ended) { ended = true; closed++; } }
            };
        }
    }
}
