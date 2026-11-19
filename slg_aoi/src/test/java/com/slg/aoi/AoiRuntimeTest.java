package com.slg.aoi;

import com.slg.aoi.MapStateSource.*;
import com.slg.core.world.Viewport;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class AoiRuntimeTest {
    private static final Viewport CENTER = new Viewport(590, 590, 610, 610);
    private static final Viewport ALL = new Viewport(0, 0, 1199, 1199);

    @Test void longRouteIsVisibleThenRecallAndReturnUpdateTheSameViewWithoutRpc() throws Exception {
        var source = new Source();
        source.put(route("a", 1, 1199));
        try (var runtime = runtime(source)) {
            runtime.start();
            ready(runtime, 1);
            assertEquals(List.of("a"), runtime.query(CENTER).get(2, TimeUnit.SECONDS).routes().stream().map(Route::marchId).toList());
            Route back = new Route("a", 2, Direction.RETURNING, 299.75, 299.75, 0, 0, 100, 200);
            source.put(back);
            ready(runtime, 2);
            assertTrue(runtime.query(CENTER).get(2, TimeUnit.SECONDS).routes().isEmpty());
            assertEquals(List.of(back), runtime.query(new Viewport(100, 100, 110, 110)).get().routes());
            source.remove("a", 3);
            ready(runtime, 3);
            assertTrue(runtime.query(ALL).get().routes().isEmpty());
        }
    }

    @Test void lostCommitNotificationAndNonMapPagesStillAdvanceByJournalSequence() throws Exception {
        var source = new Source();
        source.notify = false;
        try (var runtime = runtime(source)) {
            runtime.start(); ready(runtime, 0);
            source.empty();
            source.put(route("a", 1, 1199));
            source.empty();
            ready(runtime, 3);
            assertEquals(1, runtime.query(CENTER).get().routes().size());
        }
    }

    @Test void incompleteSnapshotIsNeverPublishedAndRetryStartsFromACompleteSnapshot() throws Exception {
        var source = new Source();
        source.put(route("a", 1, 1199));
        source.failSnapshot = true;
        try (var runtime = runtime(source)) {
            runtime.start();
            await(() -> runtime.metrics().get("state").equals("RECOVERING"));
            assertEquals(0, runtime.metrics().get("routes"));
            assertTrue(metric(runtime, "snapshotBuild", "failures") >= 1);
            assertThrows(Exception.class, () -> runtime.query(CENTER).get());
            assertTrue(metric(runtime, "queryExecution", "failures") >= 1);
            source.failSnapshot = false;
            ready(runtime, 1);
            assertEquals(1, runtime.query(CENTER).get().routes().size());
            assertTrue(metric(runtime, "snapshotBuild", "count") > metric(runtime, "snapshotBuild", "failures"));
            assertTrue(metric(runtime, "queryExecution", "count") > metric(runtime, "queryExecution", "failures"));
        }
    }

    private static long metric(AoiRuntime runtime, String name, String field) {
        return ((Number) ((Map<?, ?>) runtime.metrics().get(name)).get(field)).longValue();
    }

    @Test void prunedCursorRebuildsAndInvalidatesExistingSubscriptionGeneration() throws Exception {
        var source = new Source();
        source.objects.put("resource:650-650", new ResourceSite(650, 650, 0, "WOOD", ""));
        source.put(route("a", 1, 1199));
        var sink = new Sink();
        try (var runtime = runtime(source)) {
            runtime.start(); ready(runtime, 1);
            var first = runtime.query(CENTER).get();
            runtime.subscribe(CENTER, sink).get();
            assertTrue(sink.next().snapshot());
            synchronized (source) {
                source.put(route("b", 1, 1199));
                source.remove("a", 2);
                source.put(new ResourceSite(650, 650, 1, "WOOD", "17_1"));
                source.floor = source.high;
            }
            ready(runtime, 4);
            await(() -> ((Number) runtime.metrics().get("rebuilds")).longValue() >= 2);
            var rebuilt = runtime.query(CENTER).get();
            assertNotEquals(first.generation(), rebuilt.generation());
            assertEquals(List.of("b"), rebuilt.routes().stream().map(Route::marchId).toList());
            assertEquals(List.of(new ResourceSite(650, 650, 1, "WOOD", "17_1")), runtime.query(ALL).get().objects());
            assertNotNull(sink.resets.poll(2, TimeUnit.SECONDS));
            assertEquals(0, runtime.metrics().get("subscriptions"));
        }
    }

    @Test void sourceIdentityAndBadPageStopAtTheLastAppliedCursor() throws Exception {
        var source = new Source();
        try (var runtime = runtime(source)) {
            runtime.start(); ready(runtime, 0);
            source.badIdentity = true;
            source.empty();
            await(() -> runtime.metrics().get("state").equals("FAILED"));
            assertEquals(0L, runtime.metrics().get("appliedSeq"));
            source.badIdentity = false;
            source.put(route("a", 1, 1199));
            Thread.sleep(30);
            assertEquals(0L, runtime.metrics().get("appliedSeq"));
        }
    }

    @Test void subscriptionSnapshotAndUpdatesHaveNoGapAndDeleteRemovesTheLine() throws Exception {
        var source = new Source();
        source.put(route("a", 1, 1199));
        var sink = new Sink();
        try (var runtime = runtime(source)) {
            runtime.start(); ready(runtime, 1);
            try (var sub = runtime.subscribe(CENTER, sink).get()) {
                AoiRuntime.Update initial = sink.next();
                assertTrue(initial.snapshot());
                assertEquals(1, initial.throughSeq());
                source.empty(); // 允许跨过没有地图变化的日志，但下一包 fromSeq 仍是上一包 throughSeq。
                source.remove("a", 2);
                ready(runtime, 3);
                AoiRuntime.Update removed = sink.next();
                assertFalse(removed.snapshot());
                assertEquals(initial.generation(), removed.generation());
                assertEquals(1, removed.fromSeq());
                assertEquals(3, removed.throughSeq());
                assertEquals(List.of("a"), removed.removedIds());
                assertTrue(removed.upserts().isEmpty());
            }
            await(() -> runtime.metrics().get("subscriptions").equals(0));
        }
    }

    @Test void slowConsumerDoesNotStopIndexProgressAndMustResynchronizeAfterOverflow() throws Exception {
        var source = new Source();
        source.put(route("a", 1, 1199));
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var reset = new CompletableFuture<String>();
        AoiRuntime.ViewUpdateSink slow = new AoiRuntime.ViewUpdateSink() {
            @Override public boolean offer(AoiRuntime.Update update) {
                entered.countDown();
                try { release.await(3, TimeUnit.SECONDS); }
                catch (InterruptedException stop) { Thread.currentThread().interrupt(); }
                return true;
            }
            @Override public void resetRequired(String reason) { reset.complete(reason); }
        };
        var settings = AoiRuntime.Settings.fromEnvironment(Map.of("SLG_AOI_POLL_MS", "5", "SLG_AOI_SUBSCRIPTION_QUEUE", "2"));
        try (var runtime = new AoiRuntime(source, settings)) {
            runtime.start(); ready(runtime, 1);
            runtime.subscribe(CENTER, slow).get();
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            for (int version = 2; version <= 5; version++) {
                source.put(route("a", version, 1199));
                ready(runtime, version);
            }
            assertEquals(5, runtime.query(CENTER).get().routes().getFirst().version());
            await(() -> runtime.metrics().get("subscriptions").equals(0));
            release.countDown();
            assertTrue(reset.get(2, TimeUnit.SECONDS).contains("slow"));
            await(() -> runtime.metrics().get("pendingUpdates").equals(0));
        } finally { release.countDown(); }
    }

    @Test void snapshotReaderCanBlockWhileQueriesFailFastWithoutRunningOnTheCaller() throws Exception {
        var source = new Source();
        source.snapshotGate = new CountDownLatch(1);
        try (var runtime = runtime(source)) {
            runtime.start();
            assertTrue(source.snapshotEntered.await(2, TimeUnit.SECONDS));
            source.put(route("a", 1, 1199)); // 模拟 GAME 已提交，AOI 读取被阻塞不影响提交方。
            assertThrows(Exception.class, () -> runtime.query(CENTER).get(1, TimeUnit.SECONDS));
            source.snapshotGate.countDown();
            ready(runtime, 1);
            assertEquals(1, runtime.query(CENTER).get().routes().size());
            assertTrue(source.readerThread.startsWith("aoi-source-"));
        }
    }

    @Test void restartRebuildsAnIndexInsteadOfReusingACursorWithoutItsState() throws Exception {
        var source = new Source();
        source.put(route("a", 1, 1199));
        String oldGeneration;
        try (var runtime = runtime(source)) {
            runtime.start(); ready(runtime, 1);
            oldGeneration = runtime.query(CENTER).get().generation();
        }
        source.remove("a", 2);
        try (var runtime = runtime(source)) {
            runtime.start(); ready(runtime, 2);
            assertTrue(runtime.query(CENTER).get().routes().isEmpty());
            assertNotEquals(oldGeneration, runtime.query(CENTER).get().generation());
        }
    }

    @Test void rejectingSinkGetsAnExplicitResetAndReleasesSubscriptionBudget() throws Exception {
        var source = new Source();
        var reset = new CompletableFuture<String>();
        try (var runtime = runtime(source)) {
            runtime.start(); ready(runtime, 0);
            runtime.subscribe(CENTER, new AoiRuntime.ViewUpdateSink() {
                @Override public boolean offer(AoiRuntime.Update update) { return false; }
                @Override public void resetRequired(String reason) { reset.complete(reason); }
            }).get();
            assertTrue(reset.get(2, TimeUnit.SECONDS).contains("rejected"));
            await(() -> runtime.metrics().get("subscriptions").equals(0));
            await(() -> runtime.metrics().get("pendingUpdates").equals(0));
        }
    }

    @Test void closingCompletesQueuedQueriesInsteadOfLeavingTheirFuturesPending() throws Exception {
        var source = new Source();
        var runtime = runtime(source);
        runtime.start(); ready(runtime, 0);
        // 占满真实查询执行器，确保下面 public query 尚未运行；关停不能把其 Runnable 丢掉后留下悬空 future。
        var field = AoiRuntime.class.getDeclaredField("queries");
        field.setAccessible(true);
        var queries = (java.util.concurrent.ThreadPoolExecutor) field.get(runtime);
        var occupied = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        for (int i = 0; i < 2; i++) queries.execute(() -> {
            occupied.countDown();
            try { release.await(); } catch (InterruptedException stop) { Thread.currentThread().interrupt(); }
        });
        assertTrue(occupied.await(2, TimeUnit.SECONDS));
        var queued = runtime.query(CENTER);
        assertFalse(queued.isDone());
        runtime.close();
        release.countDown();
        assertTrue(queued.isCompletedExceptionally());
        assertEquals("CLOSED", runtime.metrics().get("state"));
        assertThrows(Exception.class, () -> runtime.query(CENTER).get());
    }

    @Test void closingDoesNotReturnWhileASourceStillOwnsItsSnapshot() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        MapStateSource source = new MapStateSource() {
            @Override public Identity identity() { return new Identity(17, "slow-dataset"); }
            @Override public long snapshot(Check check, Consumer<Route> receiver, Consumer<WorldObject> objects) throws Exception {
                entered.countDown();
                // 模拟一次暂时不能被 Java 中断的原生读取。释放后仍执行取消检查，不发布半成品。
                boolean stopped = false;
                while (release.getCount() != 0) {
                    try { release.await(); } catch (InterruptedException ignored) { stopped = true; }
                }
                if (stopped) Thread.currentThread().interrupt();
                check.run();
                return 0;
            }
            @Override public Page changes(long after, int limit) { return new Page(identity(), after, 0, 0, 0, List.of(), List.of()); }
        };
        var runtime = new AoiRuntime(source, AoiRuntime.Settings.defaults());
        runtime.start();
        assertTrue(entered.await(2, TimeUnit.SECONDS));
        var closing = CompletableFuture.runAsync(runtime::close);
        try {
            await(() -> runtime.metrics().get("state").equals("CLOSED"));
            assertFalse(closing.isDone(), "不能在来源仍持有快照时允许外层关闭原卷");
        } finally { release.countDown(); }
        closing.get(2, TimeUnit.SECONDS);
    }

    @Test void queryBudgetRefusesAnEntireCrowdedViewInsteadOfSilentlyTruncatingIt() throws Exception {
        var source = new Source();
        source.put(route("a", 1, 1199));
        source.put(route("b", 1, 1199));
        var limits = AoiRuntime.Settings.fromEnvironment(Map.of("SLG_AOI_MAX_CANDIDATES", "1", "SLG_AOI_MAX_RESULTS", "1"));
        try (var runtime = new AoiRuntime(source, limits)) {
            runtime.start(); ready(runtime, 2);
            var failure = assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> runtime.query(CENTER).get());
            assertInstanceOf(com.slg.core.world.RouteIndex.QueryLimit.class, failure.getCause());
            assertEquals("READY", runtime.metrics().get("state"));
        }
    }

    @Test void mineOccupationAndReleaseShareTheSameSubscriptionUpdateAsMarchTransitions() throws Exception {
        var source = new Source();
        ResourceSite initialMine = new ResourceSite(650, 650, 0, "WOOD", "");
        source.objects.put(initialMine.objectId(), initialMine);
        source.put(new City("17_1", 1, 0, 0, "home"));
        source.put(route("a", 1, 1199));
        var sink = new Sink();
        try (var runtime = runtime(source)) {
            runtime.start(); ready(runtime, 2);
            try (var subscription = runtime.subscribe(ALL, sink).get()) {
                var first = sink.next();
                assertTrue(first.snapshot());
                assertEquals(2, first.objectUpserts().size());
                assertEquals(3, runtime.metrics().get("subscriptionEntries"));
                var held = new ResourceSite(650, 650, 1, "WOOD", "17_1");
                source.transition("a", 2, null, held);
                ready(runtime, 3);
                var arrival = sink.next();
                assertEquals(2, arrival.fromSeq()); assertEquals(3, arrival.throughSeq());
                assertEquals(List.of("a"), arrival.removedIds());
                assertEquals(List.of(held), arrival.objectUpserts());
                assertTrue(runtime.query(ALL).get().routes().isEmpty());
                assertEquals(2, runtime.metrics().get("subscriptionEntries"));
                var free = new ResourceSite(650, 650, 2, "WOOD", "");
                Route returning = new Route("a", 3, Direction.RETURNING, 650, 650, 0, 0, 100, 200);
                source.transition("a", 3, returning, free);
                ready(runtime, 4);
                var release = sink.next();
                assertEquals(3, release.fromSeq()); assertEquals(4, release.throughSeq());
                assertEquals(List.of(returning), release.upserts());
                assertEquals(List.of(free), release.objectUpserts());
                assertEquals(1, runtime.metrics().get("cities"));
                assertEquals(1, runtime.metrics().get("resourceSites"));
            }
            await(() -> runtime.metrics().get("subscriptionEntries").equals(0));
        }
    }

    @Test void cityCreationPublishesOnlyToTheContainingViewport() throws Exception {
        var source = new Source();
        var home = new Sink();
        var middle = new Sink();
        try (var runtime = runtime(source)) {
            runtime.start(); ready(runtime, 0);
            try (var first = runtime.subscribe(new Viewport(0, 0, 32, 32), home).get();
                 var second = runtime.subscribe(CENTER, middle).get()) {
                assertTrue(home.next().objectUpserts().isEmpty());
                assertTrue(middle.next().objectUpserts().isEmpty());
                var city = new City("17_1", 1, 0, 0, "new home");
                source.put(city); ready(runtime, 1);
                var creation = home.next();
                assertEquals(List.of(city), creation.objectUpserts());
                assertEquals(0, creation.fromSeq()); assertEquals(1, creation.throughSeq());
                assertTrue(creation.upserts().isEmpty());
                assertTrue(runtime.query(CENTER).get().objects().isEmpty());
                assertNull(middle.updates.poll(50, TimeUnit.MILLISECONDS));
            }
        }
    }

    @Test void combinedSubscriptionReferencesAreReclaimedAfterRejectionAndClose() throws Exception {
        var source = new Source();
        source.objects.put("resource:650-650", new ResourceSite(650, 650, 0, "WOOD", ""));
        source.put(route("a", 1, 1199));
        var settings = AoiRuntime.Settings.fromEnvironment(Map.of("SLG_AOI_POLL_MS", "5",
                "SLG_AOI_MAX_SUBSCRIPTION_ENTRIES", "2", "SLG_AOI_MAX_RESULTS", "3"));
        try (var runtime = new AoiRuntime(source, settings)) {
            runtime.start(); ready(runtime, 1);
            var firstSink = new Sink();
            var first = runtime.subscribe(ALL, firstSink).get(); firstSink.next();
            assertEquals(2, runtime.metrics().get("subscriptionEntries"));
            assertThrows(java.util.concurrent.ExecutionException.class, () -> runtime.subscribe(ALL, new Sink()).get());
            first.close();
            await(() -> runtime.metrics().get("subscriptionEntries").equals(0));
            var nextSink = new Sink();
            try (var second = runtime.subscribe(ALL, nextSink).get()) {
                nextSink.next();
                source.put(new City("17_1", 1, 0, 0, "home")); ready(runtime, 2);
                assertNotNull(nextSink.resets.poll(2, TimeUnit.SECONDS));
                await(() -> runtime.metrics().get("subscriptions").equals(0));
                assertEquals(0, runtime.metrics().get("subscriptionEntries"));
                // 超限只结束视野，索引仍正常包含路线和两个点对象，不能丢城市来假装维持订阅。
                assertEquals(2, runtime.query(ALL).get().objects().size());
                assertEquals(1, runtime.query(ALL).get().routes().size());
            }
        }
    }

    private static AoiRuntime runtime(Source source) {
        return new AoiRuntime(source, AoiRuntime.Settings.fromEnvironment(Map.of("SLG_AOI_POLL_MS", "5")));
    }
    private static Route route(String id, long version, int target) {
        return new Route(id, version, Direction.OUTBOUND, 0, 0, target, target, 1, 100);
    }
    private static void ready(AoiRuntime runtime, long seq) throws Exception {
        await(() -> runtime.metrics().get("state").equals("READY")
                && ((Number) runtime.metrics().get("appliedSeq")).longValue() >= seq);
    }
    private static void await(BooleanSupplier condition) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() < until) Thread.sleep(5);
        assertTrue(condition.getAsBoolean(), "condition did not become true");
    }
    private static final class Sink implements AoiRuntime.ViewUpdateSink {
        final LinkedBlockingQueue<AoiRuntime.Update> updates = new LinkedBlockingQueue<>();
        final LinkedBlockingQueue<String> resets = new LinkedBlockingQueue<>();
        @Override public boolean offer(AoiRuntime.Update update) { return updates.offer(update); }
        @Override public void resetRequired(String reason) { resets.offer(reason); }
        AoiRuntime.Update next() throws Exception {
            var next = updates.poll(2, TimeUnit.SECONDS);
            assertNotNull(next, "missing map update");
            return next;
        }
    }
    /** 测试源保留完整连续日志，故意允许漏通知、截断窗口和快照失败来验证消费边界。 */
    private static final class Source implements MapStateSource {
        final Identity id = new Identity(17, "dataset-test");
        final Map<String, Route> routes = new HashMap<>();
        final Map<String, WorldObject> objects = new HashMap<>();
        final TreeMap<Long, List<Change>> journal = new TreeMap<>();
        final TreeMap<Long, List<ObjectChange>> objectJournal = new TreeMap<>();
        final CountDownLatch snapshotEntered = new CountDownLatch(1);
        volatile boolean notify = true, failSnapshot, badIdentity;
        volatile CountDownLatch snapshotGate;
        volatile String readerThread;
        Runnable wake = () -> {};
        long high, floor;
        @Override public Identity identity() { return id; }
        synchronized void put(Route route) {
            journal.put(++high, List.of(new Change(high, route.marchId(), route.version(), route)));
            routes.put(route.marchId(), route);
            if (notify) wake.run();
        }
        synchronized void remove(String id, long version) {
            journal.put(++high, List.of(new Change(high, id, version, null)));
            routes.remove(id);
            if (notify) wake.run();
        }
        synchronized void put(WorldObject object) {
            journal.put(++high, List.of());
            objectJournal.put(high, List.of(new ObjectChange(high, object.objectId(), object.version(), object)));
            objects.put(object.objectId(), object);
            if (notify) wake.run();
        }
        synchronized void transition(String id, long version, Route route, ResourceSite site) {
            journal.put(++high, List.of(new Change(high, id, version, route)));
            objectJournal.put(high, List.of(new ObjectChange(high, site.objectId(), site.version(), site)));
            if (route == null) routes.remove(id); else routes.put(id, route);
            objects.put(site.objectId(), site);
            if (notify) wake.run();
        }
        synchronized void empty() { journal.put(++high, List.of()); if (notify) wake.run(); }
        @Override public long snapshot(Check check, Consumer<Route> receiver, Consumer<WorldObject> objects) throws Exception {
            readerThread = Thread.currentThread().getName();
            long captured;
            List<Route> copy;
            List<WorldObject> pointCopy;
            synchronized (this) { captured = high; copy = List.copyOf(routes.values()); pointCopy = List.copyOf(this.objects.values()); }
            snapshotEntered.countDown();
            if (snapshotGate != null) snapshotGate.await(3, TimeUnit.SECONDS);
            for (Route route : copy) { check.run(); receiver.accept(route); }
            for (WorldObject object : pointCopy) { check.run(); objects.accept(object); }
            if (failSnapshot) throw new IOException("incomplete snapshot");
            return captured;
        }
        @Override public synchronized Page changes(long after, int limit) {
            Identity identity = badIdentity ? new Identity(18, "wrong-dataset") : id;
            if (after < floor) return new Page(identity, after, floor, high, after, List.of(), List.of());
            long through = Math.min(high, after + limit);
            List<Change> changes = new ArrayList<>();
            List<ObjectChange> objects = new ArrayList<>();
            for (long seq = after + 1; seq <= through; seq++) changes.addAll(journal.get(seq));
            for (long seq = after + 1; seq <= through; seq++) objects.addAll(objectJournal.getOrDefault(seq, List.of()));
            return new Page(identity, after, floor, high, through, changes, objects);
        }
        @Override public synchronized AutoCloseable onAdvance(Runnable signal) { wake = signal; return () -> { synchronized (this) { wake = () -> {}; } }; }
    }
}
