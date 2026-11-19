package com.slg.aoi;

import com.slg.aoi.MapStateSource.*;
import com.slg.core.world.Viewport;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class AoiIndexTest {
    private static final Identity ID = new Identity(17, "dataset-test");
    private static final Viewport ALL = new Viewport(0, 0, 1199, 1199);

    @Test void aMalformedLaterChangeCannotPublishHalfOfAPage() throws Exception {
        AoiIndex index = new AoiIndex(ID, "generation", AoiRuntime.Settings.defaults());
        index.finishSnapshot(0);
        Route valid = route("a", 1, 1199);
        Route outside = route("b", 1, 1200);
        assertThrows(BadData.class, () -> index.apply(new Page(ID, 0, 0, 2, 2,
                List.of(new Change(1, "a", 1, valid), new Change(2, "b", 1, outside)), List.of())));
        assertEquals(0, index.sequence());
        assertEquals(0, index.size());
        assertTrue(index.query(ALL).routes().isEmpty());
    }

    @Test void duplicatePageIsIgnoredAndDeletePreventsOldVersionResurrection() throws Exception {
        AoiIndex index = new AoiIndex(ID, "generation", AoiRuntime.Settings.defaults());
        index.finishSnapshot(0);
        Page add = new Page(ID, 0, 0, 1, 1, List.of(new Change(1, "a", 1, route("a", 1, 1199))), List.of());
        index.apply(add);
        assertTrue(index.apply(add).isEmpty());
        index.apply(new Page(ID, 1, 0, 2, 2, List.of(new Change(2, "a", 2, null)), List.of()));
        assertThrows(BadData.class, () -> index.apply(new Page(ID, 2, 0, 3, 3,
                List.of(new Change(3, "a", 1, route("a", 1, 1199))), List.of())));
        assertEquals(2, index.sequence());
        assertTrue(index.query(ALL).routes().isEmpty());
    }

    @Test void pageCollapsesMultipleChangesToOneFinalMutationPerMarch() throws Exception {
        AoiIndex index = new AoiIndex(ID, "generation", AoiRuntime.Settings.defaults());
        index.addSnapshot(route("a", 1, 1199));
        index.finishSnapshot(1);
        Route returning = new Route("a", 3, Direction.RETURNING, 600, 600, 0, 0, 100, 200);
        List<Change> changed = index.apply(new Page(ID, 1, 0, 4, 4, List.of(
                new Change(2, "a", 2, null), new Change(3, "a", 3, returning),
                new Change(4, "b", 1, route("b", 1, 1199)), new Change(4, "b", 2, null)), List.of())).routes();
        assertEquals(List.of(new Change(3, "a", 3, returning), new Change(4, "b", 2, null)), changed);
        assertEquals(List.of(returning), index.query(ALL).routes());
        assertEquals(4, index.sequence());
    }

    @Test void wrongSourceAndPartialOverlapDoNotAdvanceTheCursor() throws Exception {
        AoiIndex index = new AoiIndex(ID, "generation", AoiRuntime.Settings.defaults());
        index.finishSnapshot(3);
        assertThrows(BadData.class, () -> index.apply(new Page(new Identity(18, "dataset-test"), 3, 0, 4, 4, List.of(), List.of())));
        assertThrows(BadData.class, () -> index.apply(new Page(ID, 2, 0, 4, 4, List.of(), List.of())));
        assertThrows(BadData.class, () -> index.apply(new Page(ID, 3, 0, 4, 3, List.of(), List.of())));
        assertEquals(3, index.sequence());
    }

    @Test void concurrentQueriesSeeOneWholePageVersion() throws Exception {
        AoiIndex index = new AoiIndex(ID, "generation", AoiRuntime.Settings.defaults());
        for (int i = 0; i < 100; i++) {
            index.addSnapshot(route("route-" + i, 1, 1199));
            index.addSnapshot(new City("17_" + (i + 1), 1, i, i, "city"));
        }
        index.finishSnapshot(1);
        try (var threads = Executors.newFixedThreadPool(2)) {
            var writer = threads.submit(() -> {
                for (int version = 2; version <= 50; version++) {
                    var changes = new ArrayList<Change>();
                    var objects = new ArrayList<ObjectChange>();
                    for (int i = 0; i < 100; i++) changes.add(new Change(version, "route-" + i, version,
                            route("route-" + i, version, 1199)));
                    for (int i = 0; i < 100; i++) {
                        City city = new City("17_" + (i + 1), version, i, i, "city");
                        objects.add(new ObjectChange(version, city.objectId(), version, city));
                    }
                    index.apply(new Page(ID, version - 1, 0, version, version, changes, objects));
                }
                return null;
            });
            var reader = threads.submit(() -> {
                for (int i = 0; i < 100; i++) {
                    var view = index.query(ALL);
                    assertEquals(100, view.routes().size());
                    assertTrue(view.routes().stream().allMatch(route -> route.version() == view.appliedSeq()));
                    assertEquals(100, view.objects().size());
                    assertTrue(view.objects().stream().allMatch(object -> object.version() == view.appliedSeq()));
                }
            });
            writer.get(); reader.get();
        }
    }

    @Test void staticMineAtZeroAcceptsItsFirstOccupationAndSharesTheRouteCommitBoundary() throws Exception {
        AoiIndex index = new AoiIndex(ID, "generation", AoiRuntime.Settings.defaults());
        var free = new ResourceSite(650, 650, 0, "WOOD", "");
        index.addSnapshot(free);
        index.finishSnapshot(0);
        assertEquals(List.of(free), index.query(ALL).objects());
        index.apply(new Page(ID, 0, 0, 1, 1, List.of(new Change(1, "a", 1, route("a", 1, 1199))), List.of()));
        var occupied = new ResourceSite(650, 650, 1, "WOOD", "17_1");
        index.apply(new Page(ID, 1, 0, 2, 2, List.of(new Change(2, "a", 2, null)),
                List.of(new ObjectChange(2, occupied.objectId(), 1, occupied))));
        var arrived = index.query(ALL);
        assertEquals(2, arrived.appliedSeq());
        assertTrue(arrived.routes().isEmpty());
        assertEquals(List.of(occupied), arrived.objects());
        var released = new ResourceSite(650, 650, 2, "WOOD", "");
        Route returning = new Route("a", 3, Direction.RETURNING, 650, 650, 0, 0, 100, 200);
        index.apply(new Page(ID, 2, 0, 3, 3, List.of(new Change(3, "a", 3, returning)),
                List.of(new ObjectChange(3, released.objectId(), 2, released))));
        var back = index.query(ALL);
        assertEquals(3, back.appliedSeq());
        assertEquals(List.of(returning), back.routes());
        assertEquals(List.of(released), back.objects());
    }

    @Test void badObjectCannotPublishItsCompanionRouteAndSameVersionConflictsStopThePage() throws Exception {
        AoiIndex index = new AoiIndex(ID, "generation", AoiRuntime.Settings.defaults());
        var free = new ResourceSite(650, 650, 0, "WOOD", "");
        index.addSnapshot(free); index.finishSnapshot(0);
        var outside = new City("17_1", 1, 1200, 1, "outside");
        assertThrows(BadData.class, () -> index.apply(new Page(ID, 0, 0, 1, 1,
                List.of(new Change(1, "a", 1, route("a", 1, 1199))),
                List.of(new ObjectChange(1, outside.objectId(), 1, outside)))));
        assertEquals(0, index.sequence());
        assertEquals(List.of(free), index.query(ALL).objects());
        assertTrue(index.query(ALL).routes().isEmpty());
        var occupied = new ResourceSite(650, 650, 1, "WOOD", "17_1");
        index.apply(new Page(ID, 0, 0, 1, 1, List.of(), List.of(new ObjectChange(1, occupied.objectId(), 1, occupied))));
        var wrongOwner = new ResourceSite(650, 650, 1, "WOOD", "17_2");
        assertThrows(BadData.class, () -> index.apply(new Page(ID, 1, 0, 2, 2,
                List.of(new Change(2, "a", 1, route("a", 1, 1199))),
                List.of(new ObjectChange(2, wrongOwner.objectId(), 1, wrongOwner)))));
        assertEquals(1, index.sequence());
        assertEquals(List.of(occupied), index.query(ALL).objects());
        assertTrue(index.query(ALL).routes().isEmpty());
    }

    @Test void pointBordersMovesAndDeletionRemoveOldChunkReferences() throws Exception {
        AoiIndex index = new AoiIndex(ID, "generation", AoiRuntime.Settings.defaults());
        City city = new City("17_1", 1, 32, 32, "border");
        index.addSnapshot(city); index.finishSnapshot(1);
        assertEquals(List.of(city), index.query(new Viewport(32, 32, 32, 32)).objects());
        assertTrue(index.query(new Viewport(Math.nextUp(32d), 32, 33, 33)).objects().isEmpty());
        City moved = new City("17_1", 2, 1199, 1199, "border");
        index.apply(new Page(ID, 1, 0, 2, 2, List.of(), List.of(new ObjectChange(2, moved.objectId(), 2, moved))));
        assertTrue(index.query(new Viewport(0, 0, 64, 64)).objects().isEmpty());
        assertEquals(List.of(moved), index.query(new Viewport(1199, 1199, 1200, 1200)).objects());
        index.apply(new Page(ID, 2, 0, 3, 3, List.of(), List.of(new ObjectChange(3, moved.objectId(), 3, null))));
        assertTrue(index.query(ALL).objects().isEmpty());
        assertThrows(BadData.class, () -> index.apply(new Page(ID, 3, 0, 4, 4, List.of(),
                List.of(new ObjectChange(4, moved.objectId(), 2, moved)))));
        assertEquals(3, index.sequence());
        assertEquals(0, index.cityCount());
    }

    @Test void candidateAndResultBudgetsAreSharedAcrossRoutesAndPoints() {
        var candidates = AoiRuntime.Settings.fromEnvironment(Map.of("SLG_AOI_MAX_CANDIDATES", "1", "SLG_AOI_MAX_RESULTS", "1"));
        AoiIndex index = new AoiIndex(ID, "generation", candidates);
        index.addSnapshot(route("a", 1, 1199));
        index.addSnapshot(new City("17_1", 1, 10, 15, "same chunk"));
        index.finishSnapshot(1);
        // 路线只是候选、不真正经过窗口，仍消耗一次候选预算；点不能再领取一份新的预算。
        assertThrows(com.slg.core.world.RouteIndex.QueryLimit.class, () -> index.query(new Viewport(10, 15, 11, 16)));
        var results = AoiRuntime.Settings.fromEnvironment(Map.of("SLG_AOI_MAX_CANDIDATES", "10", "SLG_AOI_MAX_RESULTS", "1"));
        AoiIndex crowded = new AoiIndex(ID, "generation", results);
        crowded.addSnapshot(route("a", 1, 1199));
        crowded.addSnapshot(new City("17_1", 1, 600, 600, "visible"));
        crowded.finishSnapshot(1);
        assertThrows(com.slg.core.world.RouteIndex.QueryLimit.class, () -> crowded.query(new Viewport(599, 599, 601, 601)));
    }

    @Test void objectAndCombinedVersionBudgetsFailBeforePublishingAnyChange() throws Exception {
        var limits = AoiRuntime.Settings.fromEnvironment(Map.of("SLG_AOI_MAX_ROUTES", "1", "SLG_AOI_MAX_OBJECTS", "1",
                "SLG_AOI_MAX_TRACKED_VERSIONS", "2", "SLG_AOI_MAX_CANDIDATES", "2", "SLG_AOI_MAX_RESULTS", "2"));
        AoiIndex index = new AoiIndex(ID, "generation", limits);
        var city = new City("17_1", 1, 1, 1, "one");
        index.addSnapshot(city); index.addSnapshot(route("a", 1, 1199)); index.finishSnapshot(1);
        var extra = new City("17_2", 1, 2, 2, "two");
        assertThrows(AoiIndex.CapacityExceeded.class, () -> index.apply(new Page(ID, 1, 0, 2, 2, List.of(),
                List.of(new ObjectChange(2, extra.objectId(), 1, extra)))));
        assertThrows(AoiIndex.RebuildRequired.class, () -> index.apply(new Page(ID, 1, 0, 2, 2,
                List.of(new Change(2, "a", 2, null)), List.of(new ObjectChange(2, "city:17_99", 1, null)))));
        assertEquals(1, index.sequence());
        assertEquals(1, index.query(ALL).routes().size());
        assertEquals(List.of(city), index.query(ALL).objects());
    }

    private static Route route(String id, long version, int target) {
        return new Route(id, version, Direction.OUTBOUND, 0, 0, target, target, 1, 100);
    }
}
