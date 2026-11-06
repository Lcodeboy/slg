package com.slg.game.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class RealmDiskGuardTest {
    @TempDir Path directory;

    @Test void warningAloneAllowsNewWritesButAHardLimitRequiresTheHigherWatermarkToReopen() {
        var bytes = new AtomicLong(150);
        try (var guard = new RealmDiskGuard(200, 100, bytes::get, () -> 0)) {
            assertState(guard, "STARTING", false);
            guard.sample();
            assertState(guard, "WARNING", true);
            bytes.set(100);
            guard.sample();
            assertState(guard, "HARD_LIMIT", false);
            bytes.set(199);
            guard.sample();
            assertState(guard, "RECOVERING", false);
            bytes.set(200);
            guard.sample();
            assertState(guard, "NORMAL", true);
            bytes.set(190);
            guard.sample();
            assertState(guard, "WARNING", true);
        }
    }

    @Test void aMissingFailedOrOutdatedReadingCannotAdmitNewWrites() {
        var bytes = new AtomicLong(210);
        var now = new AtomicLong(25);
        var calls = new AtomicInteger();
        try (var guard = new RealmDiskGuard(200, 100, () -> {
            calls.incrementAndGet();
            if (bytes.get() == -1) throw new IOException("filesystem offline");
            return bytes.get();
        }, now::get)) {
            guard.sample();
            long expiry = TimeUnit.MILLISECONDS.toNanos(RealmDiskGuard.STALE_AFTER_MS);
            now.addAndGet(expiry - 1);
            assertState(guard, "NORMAL", true);
            now.incrementAndGet();
            assertState(guard, "STALE", false);
            assertEquals(1, calls.get(), "Reading metrics or gating a write must never probe the disk");
            bytes.set(150);
            guard.sample();
            assertState(guard, "RECOVERING", false);
            bytes.set(-1);
            guard.sample();
            assertState(guard, "UNKNOWN", false);
            assertNull(guard.metrics().get("usableBytes"));
            bytes.set(0);
            guard.sample();
            assertState(guard, "HARD_LIMIT", false);
            bytes.set(-2);
            guard.sample();
            assertState(guard, "UNKNOWN", false);
            bytes.set(175);
            guard.sample();
            assertState(guard, "RECOVERING", false);
            bytes.set(210);
            guard.sample();
            assertState(guard, "NORMAL", true);
        }
    }

    @Test void aProbeTakingTheEntireFreshnessWindowCannotPublishAFreshReading() {
        var now = new AtomicLong(10);
        try (var guard = new RealmDiskGuard(200, 100, () -> {
            now.addAndGet(TimeUnit.MILLISECONDS.toNanos(RealmDiskGuard.STALE_AFTER_MS));
            return 500;
        }, now::get)) {
            guard.sample();
            assertState(guard, "STALE", false);
        }
    }

    @Test void anOldSampleThatExpiresDuringTheNextProbeRequiresTheRecoveryWatermark() {
        var now = new AtomicLong();
        var calls = new AtomicInteger();
        try (var guard = new RealmDiskGuard(200, 100, () -> {
            if (calls.getAndIncrement() == 0) return 300;
            now.addAndGet(TimeUnit.SECONDS.toNanos(3));
            return 150;
        }, now::get)) {
            guard.sample();
            now.set(TimeUnit.SECONDS.toNanos(13));
            guard.sample(); // 上次读数在探测过程中达到 15 秒，此次读数只回到了两线之间。
            assertState(guard, "RECOVERING", false);
        }
    }

    @Test void aStalledSamplerNeitherBlocksAdmissionNorReopensAClosedGuard() throws Exception {
        var entered = new CountDownLatch(1);
        var finished = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var guard = new RealmDiskGuard(200, 100, () -> {
            entered.countDown();
            try { release.await(5, TimeUnit.SECONDS); }
            catch (InterruptedException stopped) { Thread.currentThread().interrupt(); }
            finally { finished.countDown(); }
            return 400;
        }, System::nanoTime);
        try (guard) {
            guard.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertState(guard, "STARTING", false);
            assertThrows(IllegalStateException.class, guard::start);
            guard.close();
            assertTrue(finished.await(5, TimeUnit.SECONDS));
            assertState(guard, "CLOSED", false);
            assertThrows(IllegalStateException.class, guard::start);
        } finally { release.countDown(); }
    }

    @Test void oneShotSamplesTheRealmFilesystemWithoutCreatingFilesOrAssumingAnUnknownPathHasSpace()
            throws Exception {
        var config = Map.of("SLG_REALM_DISK_WARN_BYTES", "2", "SLG_REALM_DISK_HARD_BYTES", "1");
        var metrics = RealmDiskGuard.observe(directory, config);
        assertEquals("NORMAL", metrics.get("state"));
        assertTrue((Long) metrics.get("usableBytes") >= 2);
        try (var paths = Files.list(directory)) { assertEquals(0, paths.count()); }
        var missing = RealmDiskGuard.observe(directory.resolve("missing"), config);
        assertEquals("UNKNOWN", missing.get("state"));
        assertEquals(true, missing.get("clientWritesLimited"));
        assertFalse(Files.exists(directory.resolve("missing")));
        assertThrows(IllegalArgumentException.class, () -> RealmDiskGuard.configure(directory,
                Map.of("SLG_REALM_DISK_HARD_BYTES", "0")));
        assertThrows(IllegalArgumentException.class, () -> RealmDiskGuard.configure(directory,
                Map.of("SLG_REALM_DISK_WARN_BYTES", "4", "SLG_REALM_DISK_HARD_BYTES", "4")));
        assertThrows(IllegalArgumentException.class, () -> RealmDiskGuard.configure(directory,
                Map.of("SLG_REALM_DISK_HARD_BYTES", "1g")));
    }

    private static void assertState(RealmDiskGuard guard, String expected, boolean open) {
        assertEquals(expected, guard.metrics().get("state"));
        assertEquals(!open, guard.metrics().get("clientWritesLimited"));
        assertEquals(open, guard.admitsClientWrite());
    }
}
