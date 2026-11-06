package com.slg.game.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class RealmVolumeTest {
    private static final int REALM = 17;
    @TempDir Path temp;

    @Test void onlyMatchingRealmAndVolumeCanReopenAnInitializedDirectory() throws Exception {
        Path path = Files.createDirectory(temp.resolve("realm"));
        UUID id = UUID.randomUUID();
        RealmVolume.initialize(path, REALM, id);

        assertThrows(Exception.class, () -> openOnce(path, REALM + 1, id));
        assertThrows(Exception.class, () -> openOnce(path, REALM, UUID.randomUUID()));
        assertThrows(Exception.class, () -> RealmVolume.initialize(path, REALM, UUID.randomUUID()));
        openOnce(path, REALM, id);
    }

    @Test void openNeverProvisionsAVolumeAndInitializationProtectsExistingFiles() throws Exception {
        UUID id = UUID.randomUUID();
        Path absent = temp.resolve("missing");
        assertThrows(Exception.class, () -> openOnce(absent, REALM, id));
        assertThrows(Exception.class, () -> RealmVolume.initialize(absent, REALM, id));
        assertFalse(Files.exists(absent));

        Path empty = Files.createDirectory(temp.resolve("empty"));
        assertThrows(Exception.class, () -> openOnce(empty, REALM, id));
        Path occupied = Files.createDirectory(temp.resolve("occupied"));
        Path marker = Files.writeString(occupied.resolve("existing-data"), "keep");
        assertThrows(Exception.class, () -> RealmVolume.initialize(occupied, REALM, id));
        assertThrows(Exception.class, () -> openOnce(occupied, REALM, id));
        assertEquals("keep", Files.readString(marker));
    }

    @Test void incompatibleOrIncompleteMetadataIsRejectedWithoutBeingRewritten() throws Exception {
        Path path = Files.createDirectory(temp.resolve("realm"));
        UUID id = UUID.randomUUID();
        RealmVolume.initialize(path, REALM, id);
        byte[] format = "meta/format".getBytes(StandardCharsets.UTF_8);
        byte[] volume = "meta/volume".getBytes(StandardCharsets.UTF_8);
        byte[] unsupported = "999".getBytes(StandardCharsets.UTF_8);
        try (Options options = new Options().setCreateIfMissing(false)) {
            try (RocksDB db = RocksDB.open(options, path.toString())) {
                assertArrayEquals("8".getBytes(StandardCharsets.UTF_8), db.get(format));
                assertArrayEquals(id.toString().getBytes(StandardCharsets.UTF_8), db.get(volume));
                db.put(format, unsupported);
            }
            assertThrows(Exception.class, () -> openOnce(path, REALM, id));
            try (RocksDB db = RocksDB.open(options, path.toString())) {
                assertArrayEquals(unsupported, db.get(format));
                db.put(format, "8".getBytes(StandardCharsets.UTF_8));
                db.delete(volume);
            }
            assertThrows(Exception.class, () -> openOnce(path, REALM, id));
            try (RocksDB db = RocksDB.open(options, path.toString())) {
                assertNull(db.get(volume), "opening an incomplete volume must not invent an identity");
            }
        }
    }

    @Test void gameRefusesAllEarlierWriteFormatsWithoutChangingTheVolume() throws Exception {
        Path path = Files.createDirectory(temp.resolve("legacy"));
        UUID id = UUID.randomUUID();
        RealmVolume.initialize(path, REALM, id);
        byte[] format = "meta/format".getBytes(StandardCharsets.US_ASCII);
        try (Options options = new Options().setCreateIfMissing(false)) {
            for (byte legacy : new byte[]{'1', '2', '3', '4', '5', '6', '7'}) {
                try (RocksDB db = RocksDB.open(options, path.toString())) { db.put(format, new byte[]{legacy}); }
                assertThrows(java.io.IOException.class, () -> openOnce(path, REALM, id));
                try (RocksDB db = RocksDB.openReadOnly(options, path.toString())) {
                    assertArrayEquals(new byte[]{legacy}, db.get(format), "Refusal must not upgrade legacy metadata");
                }
            }
        }
    }

    @Test void aVolumeWithMissingOrFalseWatermarkCannotResetItsJournal() throws Exception {
        Path path = Files.createDirectory(temp.resolve("realm"));
        UUID id = UUID.randomUUID();
        RealmVolume.initialize(path, REALM, id);
        byte[] high = "meta/journal-high".getBytes(StandardCharsets.UTF_8);
        try (Options options = new Options().setCreateIfMissing(false)) {
            try (RocksDB db = RocksDB.open(options, path.toString())) {
                db.delete(high);
            }
            assertThrows(Exception.class, () -> openOnce(path, REALM, id));
            try (RocksDB db = RocksDB.open(options, path.toString())) {
                assertNull(db.get(high));
                db.put(high, new byte[]{0, 0, 0, 0, 0, 0, 0, 9});
            }
            assertThrows(Exception.class, () -> openOnce(path, REALM, id));
        }
    }

    @Test void twoConcurrentInitializersCannotClaimTheSameDirectory() throws Exception {
        Path path = Files.createDirectory(temp.resolve("realm"));
        UUID first = UUID.randomUUID(), second = UUID.randomUUID();
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        boolean firstWon, secondWon;
        try (var pool = Executors.newFixedThreadPool(2)) {
            var one = pool.submit(initializer(path, first, ready, start));
            var two = pool.submit(initializer(path, second, ready, start));
            try {
                assertTrue(ready.await(5, TimeUnit.SECONDS));
            } finally {
                start.countDown();
            }
            firstWon = one.get(15, TimeUnit.SECONDS);
            secondWon = two.get(15, TimeUnit.SECONDS);
        }
        assertNotEquals(firstWon, secondWon, "exactly one initializer should own the volume identity");
        openOnce(path, REALM, firstWon ? first : second);
        assertThrows(Exception.class, () -> openOnce(path, REALM, firstWon ? second : first));
    }

    private static Callable<Boolean> initializer(Path path, UUID id, CountDownLatch ready,
                                                 CountDownLatch start) {
        return () -> {
            ready.countDown();
            assertTrue(start.await(5, TimeUnit.SECONDS));
            try {
                RealmVolume.initialize(path, REALM, id);
                return true;
            } catch (Exception rejected) {
                return false;
            }
        };
    }

    @Test void twoDifferentRealmsCanBeOpenWhileASecondOwnerOfEitherIsRejected() throws Exception {
        Path a = Files.createDirectory(temp.resolve("A"));
        Path b = Files.createDirectory(temp.resolve("B"));
        UUID aId = UUID.randomUUID(), bId = UUID.randomUUID();
        RealmVolume.initialize(a, REALM, aId);
        RealmVolume.initialize(b, REALM + 1, bId);
        try (RealmVolume ownerA = RealmVolume.open(a, REALM, aId);
             RealmVolume ownerB = RealmVolume.open(b, REALM + 1, bId)) {
            assertThrows(Exception.class, () -> openOnce(a, REALM, aId));
            assertThrows(Exception.class, () -> openOnce(b, REALM + 1, bId));
        }
        openOnce(a, REALM, aId);
        openOnce(b, REALM + 1, bId);
    }

    @Test void killingTheOwningProcessReleasesTheOriginalVolumeForRecovery() throws Exception {
        Path path = Files.createDirectory(temp.resolve("realm"));
        Path ready = temp.resolve("owner-ready");
        Path output = temp.resolve("owner.log");
        UUID id = UUID.randomUUID();
        RealmVolume.initialize(path, REALM, id);
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        Process owner = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx64m", "-cp", classpath, WriterProcess.class.getName(), path.toString(), id.toString(),
                ready.toString()).redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (!Files.exists(ready) && owner.isAlive() && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
            assertTrue(Files.exists(ready) && owner.isAlive(),
                    "child did not acquire the volume: " + Files.readString(output));
            assertThrows(Exception.class, () -> openOnce(path, REALM, id));
        } finally {
            owner.destroyForcibly();
            assertTrue(owner.waitFor(10, TimeUnit.SECONDS), "child JVM did not terminate");
        }
        openOnce(path, REALM, id);
        assertThrows(Exception.class, () -> openOnce(path, REALM, UUID.randomUUID()));
    }

    private static void openOnce(Path path, int realmId, UUID id) throws Exception {
        try (RealmVolume ignored = RealmVolume.open(path, realmId, id)) {
            // 走真实打开和关闭路径，同时确保误放行的测试不会泄漏 native 文件句柄。
        }
    }

    /** 子进程在获得真实写锁后发出信号。父进程持有 stdin 写端，直至强杀子进程。 */
    public static final class WriterProcess {
        public static void main(String[] args) throws Exception {
            try (RealmVolume ignored = RealmVolume.open(Path.of(args[0]), REALM, UUID.fromString(args[1]))) {
                Files.createFile(Path.of(args[2]));
                System.in.read();
            }
        }
    }
}
