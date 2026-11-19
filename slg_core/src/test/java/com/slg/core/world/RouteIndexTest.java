package com.slg.core.world;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

class RouteIndexTest {
    @Test
    void fullDiagonalIsVisibleWithoutEitherEndpointOrTheMovingUnitInView() {
        RouteIndex index = new RouteIndex(1200, 32);
        MarchRoute route = route("long", new Point(0, 0), new Point(1199, 1199), 1);
        Viewport view = new Viewport(590, 590, 610, 610);
        index.upsert(route);

        assertFalse(view.contains(route.from()));
        assertFalse(view.contains(route.to()));
        assertFalse(view.contains(route.positionAt(100)));
        assertEquals(List.of(route), index.query(view));
    }

    @Test
    void boundingBoxOverlapAloneDoesNotMakeRouteVisible() {
        RouteIndex index = new RouteIndex(1200, 32);
        index.upsert(route("diagonal", new Point(0, 0), new Point(1199, 1199), 1));
        assertTrue(index.query(new Viewport(400, 600, 420, 620)).isEmpty());
    }

    @Test
    void chunkCornersEdgesAndMapEdgesAreInclusive() {
        RouteIndex index = new RouteIndex(1200, 32);
        MarchRoute diagonal = route("diagonal", new Point(0, 0), new Point(96, 96), 1);
        index.upsert(diagonal);
        assertEquals(List.of(diagonal), index.query(new Viewport(0, 32, 32, 64)));
        assertEquals(List.of(diagonal), index.query(new Viewport(32, 0, 64, 32)));
        assertEquals(List.of(diagonal), index.query(new Viewport(32, 32, 32, 32)));
        assertTrue(index.query(new Viewport(0, 32.01, 31.99, 64)).isEmpty());

        MarchRoute edge = route("vertical", new Point(64, 0), new Point(64, 1199), 1);
        index.upsert(edge);
        assertEquals(List.of(edge), index.query(new Viewport(32, 500, 64, 520)));
        assertEquals(List.of(edge), index.query(new Viewport(64, 500, 95, 520)));
        assertEquals(List.of(edge), index.query(new Viewport(64, 1199, 64, 1300)));
        assertTrue(index.query(new Viewport(1200, 1200, 1400, 1400)).isEmpty());
    }

    @Test
    void horizontalReverseAndStationaryRoutesAreIndexed() {
        RouteIndex index = new RouteIndex(1200, 32);
        MarchRoute horizontal = route("horizontal", new Point(1199, 64), new Point(0, 64), 1);
        MarchRoute stationary = route("stationary", new Point(32, 32), new Point(32, 32), 1);
        index.upsert(horizontal);
        index.upsert(stationary);
        assertEquals(List.of(horizontal), index.query(new Viewport(500, 32, 520, 64)));
        assertEquals(List.of(stationary), index.query(new Viewport(0, 0, 32, 32)));
        assertEquals(List.of(horizontal, stationary), index.query(new Viewport(0, 0, 1199, 1199)));
    }

    @Test
    void multipleChunksProduceOneRouteAndSnapshotsAreImmutable() {
        RouteIndex index = new RouteIndex(1200, 32);
        MarchRoute diagonal = route("diagonal", new Point(0, 0), new Point(1199, 1199), 1);
        index.upsert(diagonal);
        index.upsert(diagonal);
        List<MarchRoute> snapshot = index.query(new Viewport(-100, -100, 1500, 1500));
        assertEquals(1, snapshot.size());
        assertThrows(UnsupportedOperationException.class, () -> snapshot.clear());
        assertTrue(index.remove("diagonal"));
        assertFalse(index.remove("diagonal"));
        assertEquals(1, snapshot.size());
        assertEquals(0, index.size());
    }

    @Test
    void rerouteClearsOldIndexAndRejectsStaleOrConflictingVersions() {
        RouteIndex index = new RouteIndex(1200, 32);
        MarchRoute old = route("reroute", new Point(0, 0), new Point(100, 0), 1);
        MarchRoute updated = route("reroute", new Point(1000, 1000), new Point(1100, 1100), 2);
        index.upsert(old);
        index.upsert(updated);
        assertTrue(index.query(new Viewport(40, 0, 60, 10)).isEmpty());
        assertEquals(List.of(updated), index.query(new Viewport(1040, 1040, 1060, 1060)));
        assertThrows(IllegalArgumentException.class, () -> index.upsert(old));
        assertThrows(IllegalArgumentException.class, () -> index.upsert(
                route("reroute", new Point(0, 0), new Point(10, 10), 2)));
        assertEquals(1, index.size());
    }

    @Test
    void indexedQueriesMatchFullGeometryScanAcrossThreeThousandRoutes() {
        RouteIndex index = new RouteIndex(1200, 32);
        Random random = new Random(402);
        List<MarchRoute> all = new ArrayList<>();
        for (int i = 0; i < 3_000; i++) {
            MarchRoute route = route("route-" + i, point(random), point(random), 1);
            all.add(route);
            index.upsert(route);
        }
        for (int i = 0; i < 80; i++) {
            double x = random.nextDouble() * 1150;
            double y = random.nextDouble() * 1150;
            Viewport view = new Viewport(x, y, x + 45, y + 45);
            List<String> expected = all.stream()
                    .filter(route -> RouteIndex.intersects(route.from(), route.to(), view))
                    .map(MarchRoute::marchId).sorted().toList();
            assertEquals(expected, index.query(view).stream().map(MarchRoute::marchId).toList());
        }
    }

    @Test
    void concurrentReadersAndWritersObserveConsistentIndexSnapshots() throws Exception {
        RouteIndex index = new RouteIndex(1200, 32);
        Viewport all = new Viewport(0, 0, 1199, 1199);
        List<Callable<Void>> work = new ArrayList<>();
        for (int writer = 0; writer < 4; writer++) {
            int writerId = writer;
            work.add(() -> {
                for (int i = 0; i < 100; i++) {
                    String id = writerId + ":" + i;
                    index.upsert(route(id, new Point(0, i), new Point(1199, i), 1));
                    index.upsert(route(id, new Point(i, 0), new Point(i, 1199), 2));
                    if (i % 2 == 0) index.remove(id);
                }
                return null;
            });
        }
        for (int reader = 0; reader < 2; reader++) {
            work.add(() -> {
                for (int i = 0; i < 100; i++) {
                    List<MarchRoute> snapshot = index.query(all);
                    assertEquals(snapshot.size(), snapshot.stream().map(MarchRoute::marchId).distinct().count());
                }
                return null;
            });
        }
        try (var executor = Executors.newFixedThreadPool(6)) {
            for (var future : executor.invokeAll(work)) future.get();
        }
        assertEquals(200, index.size());
        assertEquals(200, index.query(all).size());
    }

    @Test
    void invalidGeometryTimingAndCoordinatesAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> new Point(Double.NaN, 1));
        assertThrows(IllegalArgumentException.class, () -> new Viewport(3, 0, 2, 10));
        assertThrows(IllegalArgumentException.class, () -> new RouteIndex(1200, 0));
        assertThrows(IllegalArgumentException.class, () -> new MarchRoute("a", new Point(0, 0), new Point(1, 1), 1, 1, 1));
        RouteIndex index = new RouteIndex(1200, 32);
        assertThrows(IllegalArgumentException.class, () -> index.upsert(route("outside", new Point(1200, 0), new Point(1, 1), 1)));
        MarchRoute route = route("time", new Point(0, 0), new Point(100, 100), 1);
        assertEquals(route.from(), route.positionAt(-1));
        assertEquals(new Point(50, 50), route.positionAt(500));
        assertEquals(route.to(), route.positionAt(Long.MAX_VALUE));
    }

    private static Point point(Random random) {
        return new Point(random.nextDouble() * 1199, random.nextDouble() * 1199);
    }

    private static MarchRoute route(String id, Point from, Point to, long version) {
        return new MarchRoute(id, from, to, 0, 1_000, version);
    }
}
