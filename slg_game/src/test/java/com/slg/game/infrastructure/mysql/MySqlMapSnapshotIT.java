package com.slg.game.infrastructure.mysql;

import com.slg.aoi.MapSnapshotSource;
import com.slg.aoi.MapStateSource;
import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.game.aoi.MySqlMapStateSource;
import com.slg.game.application.commit.CommitResult;
import com.slg.game.application.port.*;
import com.slg.game.domain.persistence.*;
import com.slg.game.infrastructure.mysql.mapper.StateMapper;
import com.slg.game.infrastructure.mysql.row.StateRows;
import java.time.Duration;
import java.util.*;
import org.apache.ibatis.scripting.defaults.DefaultParameterHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.*;

/** 真 MySQL 的地图读取边界：跨页/跨对象仍共享 H；不借端口夹具冒充 SQL/MVCC 证明。 */
@Timeout(60)
class MySqlMapSnapshotIT extends AuthoritySchemaFixture {
    private static final String PLAYER = "1_7";
    private static final ReadBudget BUDGET = new ReadBudget(10_000, 16L * 1024 * 1024, Duration.ofSeconds(20));
    private MySqlRealmStorage storage;
    private WriterToken writer;

    @BeforeEach void wire() throws Exception {
        storage = new MySqlRealmStorage(pool, 5, Duration.ofSeconds(30));
        writer = storage.ownership().acquire(identity, UUID.randomUUID());
    }

    @Test void movingPagesAndAllPublicObjectsKeepOneViewWhileAnotherConnectionCommits() throws Exception {
        var seed = movingRows();
        seed.add(new PlayerState(PLAYER, 7, 1, 1, "旧城"));
        seed.add(new GatherSiteState(150, 150, null, null, 0));
        seed.add(new ResourceState(PLAYER, ResourceType.WOOD, 0));
        commit("seed", seed.stream().map(state -> change(state, 0)).toList());
        var source = new MySqlMapStateSource(storage.states(), storage.events(), identity, BUDGET, BUDGET);
        var routes = new ArrayList<MapStateSource.Route>();
        var objects = new ArrayList<MapStateSource.WorldObject>();

        long high = source.snapshot(() -> {}, new MapSnapshotSource.SnapshotReceiver() {
            @Override public void begin(long value) throws Exception {
                assertEquals(1, value);
                // begin 已在读连接建立 RR 视图。另一写连接同时完成根/明细和事件，旧视图不可混入它。
                commit("during-snapshot", List.of(change(march("z000", MarchState.Status.COMPLETED), 1),
                        change(march("new-route", MarchState.Status.OUTBOUND), 0),
                        change(new PlayerState(PLAYER, 7, 1, 1, "新城"), 1),
                        change(new GatherSiteState(150, 150, PLAYER, "new-route", 1), 1)));
            }
            @Override public void route(MapStateSource.Route route) { routes.add(route); }
            @Override public void object(MapStateSource.WorldObject object) { objects.add(object); }
        });
        assertEquals(1, high);
        // 两个状态均跨过128根分页；返程ID小于全部去程ID，能识别错误复用上一段游标。
        assertEquals(260, routes.size());
        assertEquals(260, routes.stream().map(MapStateSource.Route::marchId).distinct().count());
        assertEquals("z000", routes.getFirst().marchId());
        assertEquals("a000", routes.get(130).marchId());
        assertTrue(routes.stream().allMatch(route -> route.version() == 1));
        assertFalse(routes.stream().anyMatch(route -> route.marchId().equals("new-route")));
        assertEquals("旧城", city(objects).name());
        assertEquals(1, city(objects).version());
        assertEquals(3, objects.stream().filter(MapStateSource.ResourceSite.class::isInstance).count());
        assertTrue(objects.stream().filter(MapStateSource.ResourceSite.class::isInstance)
                .map(MapStateSource.ResourceSite.class::cast).allMatch(site -> site.occupantPlayerId().isEmpty()));

        var changes = source.changes(1, 128);
        assertEquals(2, changes.throughSeq());
        assertEquals(2, changes.changes().size());
        assertEquals(2, changes.objectChanges().size());
        assertTrue(changes.changes().stream().allMatch(change -> change.sequence() == 2));
        assertTrue(changes.objectChanges().stream().allMatch(change -> change.sequence() == 2));
        assertNull(changes.changes().stream().filter(change -> change.marchId().equals("z000")).findFirst().orElseThrow().route());
        var freshRoutes = new ArrayList<MapStateSource.Route>();
        var freshObjects = new ArrayList<MapStateSource.WorldObject>();
        assertEquals(2, source.snapshot(() -> {}, freshRoutes::add, freshObjects::add));
        assertEquals(260, freshRoutes.size());
        assertEquals("新城", city(freshObjects).name());
        assertEquals(2, city(freshObjects).version());
        assertEquals(PLAYER, freshObjects.stream().filter(MapStateSource.ResourceSite.class::isInstance)
                .map(MapStateSource.ResourceSite.class::cast).filter(site -> site.x() == 150).findFirst().orElseThrow().occupantPlayerId());

        // 私有资产决定仍推进地图游标，不让过滤后的DTO数量伪造日志进度。
        commit("private", List.of(change(new ResourceState(PLAYER, ResourceType.WOOD, 10), 1)));
        var privatePage = source.changes(2, 128);
        assertEquals(3, privatePage.throughSeq());
        assertTrue(privatePage.changes().isEmpty());
        assertTrue(privatePage.objectChanges().isEmpty());
    }

    @Test void movingSegmentsShareChildBudgetAndUseTheMovingIndex() throws Exception {
        commit("seed", movingRows().stream().map(state -> change(state, 0)).toList());
        var seen = new ArrayList<RealmStateReader.StateEntry>();
        // 每条行军是根+兵种+两个英雄+cargo，共5行；652只够metadata和全部130条去程。
        try (var snapshot = storage.states().openSnapshot(identity, new ReadBudget(652, BUDGET.maxBytes(), BUDGET.timeout()))) {
            var failure = assertThrows(StorageAccessException.class, () -> snapshot.scanMovingMarches(seen::add));
            assertEquals(StorageAccessException.Code.READ_BUDGET_EXCEEDED, failure.code());
            assertEquals(130, seen.size());
            assertThrows(IllegalStateException.class, snapshot::highSequence);
        }
        for (var status : List.of(MarchState.Status.OUTBOUND, MarchState.Status.RETURNING)) {
            try (var session = sessions.openRead()) {
                var statement = session.getConfiguration().getMappedStatement(StateMapper.class.getName() + ".marches");
                var parameters = Map.of("q", StateRows.Query.moving(1, new ObjectKey.March("a050"), status.code(), 128));
                var bound = statement.getBoundSql(parameters);
                try (var explain = session.getConnection().prepareStatement("EXPLAIN " + bound.getSql())) {
                    new DefaultParameterHandler(statement, parameters, bound).setParameters(explain);
                    try (var rows = explain.executeQuery()) {
                        assertTrue(rows.next());
                        assertEquals("ix_march_moving", rows.getString("key"));
                        assertFalse(Objects.toString(rows.getString("Extra"), "").toLowerCase(Locale.ROOT).contains("filesort"));
                    }
                }
            }
        }
    }

    private static ArrayList<ObjectState> movingRows() {
        var rows = new ArrayList<ObjectState>();
        for (int i = 0; i < 130; i++) {
            rows.add(march("z%03d".formatted(i), MarchState.Status.OUTBOUND));
            rows.add(march("a%03d".formatted(i), MarchState.Status.RETURNING));
        }
        rows.add(march("completed", MarchState.Status.COMPLETED));
        rows.add(march("gathering", MarchState.Status.GATHERING));
        return rows;
    }
    private static MarchState march(String id, MarchState.Status status) {
        return new MarchState(id, PLAYER, status, MarchState.Mission.GATHER, 0, 0, 150, 150,
                100, 1000, 1, Map.of(TroopType.INFANTRY, 10L), List.of("hero-a", "hero-b"),
                status == MarchState.Status.COMPLETED ? null : "task-" + id, "rules", Map.of(ResourceType.WOOD, 20L));
    }
    private static ObjectChange change(ObjectState state, long version) { return new ObjectChange(state.key(), version, state); }
    private void commit(String id, List<ObjectChange> changes) throws Exception {
        var decision = new CommitDecision(new RequestKey("map-it", id), id, "MAP_IT", "{}", "{}", changes, Map.of());
        var result = assertInstanceOf(RealmTransaction.CommitAttempt.Settled.class, storage.transactions().commit(writer, decision));
        assertInstanceOf(CommitResult.Committed.class, result.result());
    }
    private static MapStateSource.City city(List<MapStateSource.WorldObject> objects) {
        return objects.stream().filter(MapStateSource.City.class::isInstance).map(MapStateSource.City.class::cast).findFirst().orElseThrow();
    }
}
