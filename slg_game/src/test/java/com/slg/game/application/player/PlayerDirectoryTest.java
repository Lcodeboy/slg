package com.slg.game.application.player;

import com.slg.game.application.port.StorageAccessException;
import com.slg.game.application.read.ReadRejectedException;
import com.slg.game.domain.persistence.RealmIdentity;
import com.slg.game.domain.player.PlayerSnapshot;
import java.time.Duration;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PlayerDirectoryTest {
    private static final RealmIdentity REALM = new RealmIdentity(1, UUID.randomUUID());
    private static final long SNAPSHOT = 100;
    private static final long MAX_SNAPSHOT = 1000;
    private static final long ENTRY = PlayerDirectory.ENTRY_METADATA_BYTES;

    @Test void mergesLoadsAndCallerCancellationDoesNotReleaseActualLoad() {
        var pending = new ArrayList<CompletableFuture<PlayerSnapshot>>();
        var directory = directory(pending, 2, 3, 5000);
        var pin = directory.pin(7);
        var first = directory.load(7);
        var second = directory.load(7);
        assertSame(first, second);
        assertEquals(1, pending.size());
        assertTrue(first.toCompletableFuture().cancel(true));
        assertEquals(ENTRY + MAX_SNAPSHOT, directory.statistics().chargedBytes());
        assertEquals(1, directory.statistics().inFlightLoads());
        pending.getFirst().complete(absent(7, 8));
        assertEquals(absent(7, 8), second.toCompletableFuture().join());
        assertEquals(ENTRY + SNAPSHOT, directory.statistics().chargedBytes());
        assertEquals(0, directory.statistics().inFlightLoads());
        assertEquals(absent(7, 8), directory.load(7).toCompletableFuture().join());
        assertEquals(1, pending.size());
        pin.close(); pin.close();
        assertEquals(0, directory.statistics().pins());
        directory.drain().toCompletableFuture().join();
    }

    @Test void entryAndPinLimitsNeverEvictPinnedOrInFlightPlayers() {
        var pending = new ArrayList<CompletableFuture<PlayerSnapshot>>();
        var directory = directory(pending, 1, 2, 5000);
        var first = directory.pin(7);
        var second = directory.pin(7);
        assertEquals(PlayerCacheException.Reason.CAPACITY, assertThrows(PlayerCacheException.class,
                () -> directory.pin(7)).reason());
        var load = directory.load(7);
        first.close(); second.close();
        assertEquals(PlayerCacheException.Reason.CAPACITY, assertThrows(PlayerCacheException.class,
                () -> directory.pin(8)).reason());
        pending.getFirst().complete(absent(7, 1));
        load.toCompletableFuture().join();
        var replacement = directory.pin(8);
        assertEquals(1, directory.statistics().entries());
        assertEquals(ENTRY, directory.statistics().chargedBytes());
        assertEquals(PlayerCacheException.Reason.NOT_PINNED, assertThrows(PlayerCacheException.class,
                () -> directory.load(7)).reason());
        replacement.close(); directory.drain().toCompletableFuture().join();
    }

    @Test void invalidationRetainsOldReservationAndSupersedesOldResult() {
        var pending = new ArrayList<CompletableFuture<PlayerSnapshot>>();
        var directory = directory(pending, 1, 1, 3000);
        var pin = directory.pin(7);
        var old = directory.load(7);
        directory.invalidate(7);
        var fresh = directory.load(7);
        assertEquals(ENTRY + MAX_SNAPSHOT * 2, directory.statistics().chargedBytes());
        pending.get(1).complete(absent(7, 20));
        assertEquals(absent(7, 20), fresh.toCompletableFuture().join());
        assertEquals(ENTRY + MAX_SNAPSHOT + SNAPSHOT, directory.statistics().chargedBytes());
        pending.getFirst().complete(absent(7, 10));
        assertEquals(PlayerCacheException.Reason.SUPERSEDED, assertInstanceOf(PlayerCacheException.class,
                failure(old)).reason());
        assertEquals(absent(7, 20), directory.load(7).toCompletableFuture().join());
        assertEquals(ENTRY + SNAPSHOT, directory.statistics().chargedBytes());
        pin.close(); directory.drain().toCompletableFuture().join();
    }

    @Test void oldLoadCannotOverwritePublicationAndInvalidationRetainsSequenceFloor() {
        var pending = new ArrayList<CompletableFuture<PlayerSnapshot>>();
        var directory = directory(pending, 1, 1, 3000);
        var pin = directory.pin(7);
        var old = directory.load(7);
        directory.publish(absent(7, 30));
        pending.getFirst().complete(absent(7, 40));
        assertEquals(PlayerCacheException.Reason.SUPERSEDED, assertInstanceOf(PlayerCacheException.class,
                failure(old)).reason());
        assertEquals(absent(7, 30), directory.load(7).toCompletableFuture().join());
        directory.invalidate(7);
        var stale = directory.load(7);
        pending.get(1).complete(absent(7, 29));
        assertEquals(StorageAccessException.Code.INVALID_DATA, assertInstanceOf(StorageAccessException.class,
                failure(stale)).code());
        var retry = directory.load(7);
        pending.get(2).complete(absent(7, 31));
        assertEquals(absent(7, 31), retry.toCompletableFuture().join());
        assertThrows(IllegalArgumentException.class, () -> directory.publish(absent(7, 30)));
        assertThrows(IllegalArgumentException.class, () -> directory.publish(PlayerSnapshot.absent(
                new RealmIdentity(1, UUID.randomUUID()), 7, 32)));
        pin.close(); directory.drain().toCompletableFuture().join();
    }

    @Test void byteLimitRejectsNewLoadWithoutReleasingOldInvalidatedLoad() {
        var pending = new ArrayList<CompletableFuture<PlayerSnapshot>>();
        var directory = directory(pending, 1, 1, ENTRY + MAX_SNAPSHOT);
        var pin = directory.pin(7);
        var old = directory.load(7);
        directory.invalidate(7);
        assertEquals(PlayerCacheException.Reason.CAPACITY, assertThrows(PlayerCacheException.class,
                () -> directory.load(7)).reason());
        assertEquals(1, pending.size());
        pending.getFirst().complete(absent(7, 1));
        assertInstanceOf(PlayerCacheException.class, failure(old));
        var retry = directory.load(7);
        pending.get(1).complete(absent(7, 2));
        assertEquals(absent(7, 2), retry.toCompletableFuture().join());
        pin.close(); directory.drain().toCompletableFuture().join();
    }

    @Test void idleEvictionUsesMonotonicTtlAndSkipsLivePins() {
        var clock = new AtomicLong();
        var directory = new PlayerDirectory(REALM, account -> CompletableFuture.completedFuture(absent(account, 1)),
                new PlayerCacheLimits(2, 2, 5000, MAX_SNAPSHOT, Duration.ofNanos(10)), ignored -> SNAPSHOT, clock::get);
        var first = directory.pin(7);
        directory.load(7).toCompletableFuture().join(); first.close();
        var second = directory.pin(8);
        clock.set(9); assertEquals(0, directory.evictIdle());
        clock.set(10); assertEquals(1, directory.evictIdle());
        assertEquals(1, directory.statistics().entries());
        second.close(); clock.set(20); assertEquals(1, directory.evictIdle());
        assertEquals(0, directory.statistics().chargedBytes());
        directory.drain().toCompletableFuture().join();
    }

    @Test void loadFailuresAreExplicitAndNeverBecomeAbsentProfiles() {
        var unavailable = new PlayerDirectory(REALM, account -> { throw new ReadRejectedException(ReadRejectedException.Reason.CAPACITY); },
                new PlayerCacheLimits(1, 1, 2000, MAX_SNAPSHOT, Duration.ofSeconds(1)), ignored -> SNAPSHOT);
        var pin = unavailable.pin(7);
        assertEquals(PlayerCacheException.Reason.CAPACITY, assertInstanceOf(PlayerCacheException.class,
                failure(unavailable.load(7))).reason());
        assertEquals(ENTRY, unavailable.statistics().chargedBytes());
        pin.close(); unavailable.drain().toCompletableFuture().join();
        var invalid = new PlayerDirectory(REALM, account -> CompletableFuture.completedFuture(absent(account, 1)),
                new PlayerCacheLimits(1, 1, 2000, MAX_SNAPSHOT, Duration.ofSeconds(1)), ignored -> MAX_SNAPSHOT + 1);
        pin = invalid.pin(7);
        assertEquals(StorageAccessException.Code.INVALID_DATA, assertInstanceOf(StorageAccessException.class,
                failure(invalid.load(7))).code());
        assertEquals(ENTRY, invalid.statistics().chargedBytes());
        pin.close(); invalid.drain().toCompletableFuture().join();
    }

    @Test void callbacksRunOutsideDirectoryLockAndDrainWaitsForTheirRealReturn() throws Exception {
        var source = new CompletableFuture<PlayerSnapshot>();
        var directory = new PlayerDirectory(REALM, account -> source,
                new PlayerCacheLimits(1, 1, 2000, MAX_SNAPSHOT, Duration.ofSeconds(1)), ignored -> SNAPSHOT);
        var pin = directory.pin(7);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var load = directory.load(7);
        var callback = load.thenRun(() -> { entered.countDown(); await(release); });
        var completer = new Thread(() -> source.complete(absent(7, 1)));
        completer.start();
        try {
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            // 当前测试线程能取得锁，证明通知线程未在目录锁内调用外部回调。
            assertEquals(ENTRY + MAX_SNAPSHOT, directory.statistics().chargedBytes());
            assertEquals(1, directory.statistics().inFlightLoads());
            pin.close();
            var drained = directory.drain().toCompletableFuture();
            assertFalse(drained.isDone());
            assertEquals(PlayerCacheException.Reason.CLOSED, assertThrows(PlayerCacheException.class,
                    () -> directory.pin(7)).reason());
            release.countDown();
            callback.toCompletableFuture().get(2, TimeUnit.SECONDS);
            drained.get(2, TimeUnit.SECONDS);
            assertEquals(0, directory.statistics().chargedBytes());
            assertTrue(directory.statistics().closed());
        } finally { release.countDown(); completer.join(2000); }
    }

    @Test void drainingStillAllowsExistingPinToReloadAndPublishRepair() {
        var pending = new ArrayList<CompletableFuture<PlayerSnapshot>>();
        var directory = directory(pending, 1, 1, 3000);
        var pin = directory.pin(7);
        var drained = directory.drain().toCompletableFuture();
        assertFalse(drained.isDone());
        var load = directory.load(7);
        pending.getFirst().complete(absent(7, 3));
        assertEquals(absent(7, 3), load.toCompletableFuture().join());
        directory.publish(absent(7, 4));
        assertFalse(drained.isDone());
        pin.close(); drained.join();
        assertEquals(PlayerCacheException.Reason.CLOSED, assertThrows(PlayerCacheException.class,
                () -> directory.load(7)).reason());
    }

    private static PlayerDirectory directory(ArrayList<CompletableFuture<PlayerSnapshot>> pending,
                                              int entries, int pins, long bytes) {
        return new PlayerDirectory(REALM, account -> {
            var future = new CompletableFuture<PlayerSnapshot>(); pending.add(future); return future;
        }, new PlayerCacheLimits(entries, pins, bytes, MAX_SNAPSHOT, Duration.ofSeconds(1)), ignored -> SNAPSHOT);
    }
    private static PlayerSnapshot absent(long account, long sequence) { return PlayerSnapshot.absent(REALM, account, sequence); }
    private static Throwable failure(CompletionStage<?> stage) {
        return assertThrows(CompletionException.class, () -> stage.toCompletableFuture().join()).getCause();
    }
    private static void await(CountDownLatch latch) {
        try { assertTrue(latch.await(2, TimeUnit.SECONDS)); }
        catch (InterruptedException error) { Thread.currentThread().interrupt(); throw new AssertionError(error); }
    }
}
