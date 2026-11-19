package com.slg.game.application.player;

import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.game.application.port.*;
import com.slg.game.application.read.BoundedReadExecutor;
import com.slg.game.application.read.ReadExecutionLimits;
import com.slg.game.domain.persistence.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PlayerSnapshotLoaderTest {
    private static final RealmIdentity REALM = new RealmIdentity(1, UUID.randomUUID());
    private static final ReadBudget BUDGET = new ReadBudget(100, 8192, Duration.ofSeconds(2));

    @Test void loadsAccountAndOwnedAssetsFromOneWorkerSnapshotUsingActiveMarchIndex() throws Exception {
        var reader = completeReader();
        // 跨区后仍按永久playerId查资产，不按当前区重新推导另一个ID。
        reader.profile = Optional.of(row(new PlayerState("2_7", 7, 2, 1, "Player")));
        reader.rows.replaceAll((kind, ignored) -> new ArrayList<>());
        addAssets(reader, "2_7");
        reader.active.add(row(new MarchState("m1", "2_7", MarchState.Status.OUTBOUND, MarchState.Mission.GATHER,
                0, 0, 2, 3, 1, 2, 1, Map.of(TroopType.INFANTRY, 10L), List.of(), "t1", "rules1", Map.of())));
        try (var reads = reads()) {
            var loaded = PlayerSnapshotLoader.using(REALM, reader, reads, BUDGET).load(7)
                    .toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertEquals("2_7", loaded.playerId());
            assertEquals(50, loaded.highSequence());
            assertEquals(1, loaded.resources().size());
            assertEquals(3, loaded.troops().size());
            assertEquals(1, loaded.heroes().size());
            assertEquals(1, loaded.activeMarches().size());
            assertEquals(1, reader.opened.get());
            assertEquals(1, reader.closed.get());
            assertEquals(List.of("account:7", "RESOURCE:2_7", "TROOP:2_7", "HERO:2_7", "active:2_7"), reader.calls);
        }
    }

    @Test void missingProfileRequiresEveryOwnedKindToBeEmptyAndDoesNotInventRows() throws Exception {
        var reader = new Reader();
        try (var reads = reads()) {
            var loaded = PlayerSnapshotLoader.using(REALM, reader, reads, BUDGET).load(7)
                    .toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertTrue(loaded.profile().isEmpty());
            assertTrue(loaded.ownedStates().isEmpty());
            assertEquals(Map.of(new ObjectKey.Player("1_7"), 0L), loaded.readVersions());
            assertEquals(ObjectKind.values().length, reader.calls.size());
            for (ObjectKind kind : ObjectKind.values()) if (kind != ObjectKind.PLAYER)
                assertTrue(reader.calls.contains(kind + ":1_7"));
            assertFalse(reader.calls.contains("active:1_7"));
            assertEquals(1, reader.closed.get());
        }
    }

    @Test void orphanCompletedHistoryRejectsMissingProfileInsteadOfCreatingNewPlayer() {
        var reader = new Reader();
        reader.rows.put(ObjectKind.TASK, new ArrayList<>(List.of(row(new TaskState("t1", "1_7", "m1",
                TaskState.Type.MARCH_RETURN, TaskState.Status.DONE, 1, 1, null)))));
        try (var reads = reads()) {
            assertFailure(reader, reads, StorageAccessException.Code.INVALID_DATA);
            assertEquals(1, reader.closed.get());
        }
    }

    @Test void duplicateOrMissingRequiredRowsRejectWholeSnapshot() {
        var duplicate = completeReader();
        duplicate.rows.get(ObjectKind.RESOURCE).add(duplicate.rows.get(ObjectKind.RESOURCE).getFirst());
        try (var reads = reads()) { assertFailure(duplicate, reads, StorageAccessException.Code.INVALID_DATA); }
        var incomplete = completeReader();
        incomplete.rows.get(ObjectKind.TROOP).removeFirst();
        try (var reads = reads()) { assertFailure(incomplete, reads, StorageAccessException.Code.INVALID_DATA); }
        var wrongOwner = completeReader();
        wrongOwner.rows.put(ObjectKind.HERO, new ArrayList<>(List.of(row(new HeroState("1_8", "sergey", null)))));
        try (var reads = reads()) { assertFailure(wrongOwner, reads, StorageAccessException.Code.INVALID_DATA); }
    }

    @Test void budgetFailureClosesSnapshotAndPreservesTypedFailureWithoutPartialCache() {
        var reader = completeReader();
        reader.failKind = ObjectKind.HERO;
        try (var reads = reads()) {
            assertFailure(reader, reads, StorageAccessException.Code.READ_BUDGET_EXCEEDED);
            assertEquals(1, reader.opened.get());
            assertEquals(1, reader.closed.get());
            assertFalse(reader.calls.contains("active:1_7"));
        }
    }

    private static void assertFailure(Reader reader, BoundedReadExecutor reads, StorageAccessException.Code code) {
        var stage = PlayerSnapshotLoader.using(REALM, reader, reads, BUDGET).load(7);
        var error = assertThrows(CompletionException.class, () -> stage.toCompletableFuture().join()).getCause();
        assertEquals(code, assertInstanceOf(StorageAccessException.class, error).code());
    }
    private static Reader completeReader() {
        var reader = new Reader();
        reader.profile = Optional.of(row(new PlayerState("1_7", 7, 1, 1, "Player")));
        addAssets(reader, "1_7"); return reader;
    }
    private static void addAssets(Reader reader, String owner) {
        reader.rows.put(ObjectKind.RESOURCE, new ArrayList<>(List.of(row(new ResourceState(owner, ResourceType.WOOD, 100)))));
        reader.rows.put(ObjectKind.TROOP, new ArrayList<>());
        for (TroopType type : TroopType.values()) reader.rows.get(ObjectKind.TROOP).add(row(new TroopState(owner, type, 100, 0, 0)));
        reader.rows.put(ObjectKind.HERO, new ArrayList<>(List.of(row(new HeroState(owner, "sergey", null)))));
    }
    private static VersionedState row(ObjectState state) { return new VersionedState(1, state); }
    private static BoundedReadExecutor reads() {
        return new BoundedReadExecutor(new ReadExecutionLimits(1, 2, 4096, 1, 1, Duration.ofSeconds(2)));
    }

    private static final class Reader implements RealmStateReader {
        private final AtomicInteger opened = new AtomicInteger();
        private final AtomicInteger closed = new AtomicInteger();
        private final List<String> calls = new ArrayList<>();
        private Optional<VersionedState> profile = Optional.empty();
        private final Map<ObjectKind, List<VersionedState>> rows = new EnumMap<>(ObjectKind.class);
        private final List<VersionedState> active = new ArrayList<>();
        private ObjectKind failKind;

        @Override public Snapshot openSnapshot(RealmIdentity identity, ReadBudget budget) {
            assertEquals(REALM, identity); assertSame(BUDGET, budget); opened.incrementAndGet();
            Thread owner = Thread.currentThread();
            assertTrue(owner.getName().contains("-worker-"));
            return new Snapshot() {
                private boolean finished;
                private void check() { assertSame(owner, Thread.currentThread()); assertFalse(finished); }
                @Override public RealmIdentity identity() { check(); return identity; }
                @Override public long highSequence() { check(); return 50; }
                @Override public Optional<VersionedState> find(ObjectKey key) { throw new AssertionError("Use account index"); }
                @Override public Optional<VersionedState> findPlayerByAccount(long accountId) {
                    check(); calls.add("account:" + accountId); return profile;
                }
                @Override public void scan(ObjectKind kind, Consumer<StateEntry> consumer) { throw new AssertionError("No realm scan"); }
                @Override public void scanOwned(ObjectKind kind, String playerId, Consumer<StateEntry> consumer) throws StorageAccessException {
                    check(); calls.add(kind + ":" + playerId);
                    if (kind == failKind) throw new StorageAccessException(StorageAccessException.Code.READ_BUDGET_EXCEEDED, "Test budget");
                    for (VersionedState value : rows.getOrDefault(kind, List.of())) consumer.accept(new StateEntry(value.state().key(), value));
                }
                @Override public void scanMovingMarches(Consumer<StateEntry> consumer) { throw new UnsupportedOperationException(); }
                @Override public void scanActiveMarches(String playerId, Consumer<StateEntry> consumer) {
                    check(); calls.add("active:" + playerId);
                    for (VersionedState value : active) consumer.accept(new StateEntry(value.state().key(), value));
                }
                @Override public void close() { check(); finished = true; closed.incrementAndGet(); }
            };
        }
    }
}
