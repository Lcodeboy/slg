package com.slg.game.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

class RealmSnapshotReadTest {
    @TempDir Path directory;

    @Test void slowSnapshotAllowsCommitsAndKeepsNativeResourcesUntilItFinishes() throws Exception {
        UUID id = UUID.randomUUID();
        RealmVolume.initialize(directory, 1, id);
        try (var workers = Executors.newFixedThreadPool(3);
             var volume = RealmVolume.open(directory, 1, id)) {
            RealmJournal journal = volume.journal();
            journal.commit(change("A", 0, "1"));
            var captured = new CountDownLatch(1);
            var finishRead = new CountDownLatch(1);
            var slow = workers.submit(() -> journal.readSnapshot("player:A", (db, options, key) -> {
                captured.countDown();
                await(finishRead);
                return db.get(options, key);
            }));
            try {
                assertTrue(captured.await(5, TimeUnit.SECONDS));
                // B 是另一个玩家；随后再改 A，以辨别读到的是真实旧快照而非新值。
                assertEquals(2L, workers.submit(() -> journal.commit(change("B", 0, "20")))
                        .get(5, TimeUnit.SECONDS).sequence());
                assertEquals(3L, workers.submit(() -> journal.commit(change("A", 1, "2")))
                        .get(5, TimeUnit.SECONDS).sequence());
                var closing = new CountDownLatch(1);
                var close = workers.submit(() -> { closing.countDown(); volume.close(); });
                assertTrue(closing.await(5, TimeUnit.SECONDS));
                assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));
                finishRead.countDown();
                var old = slow.get(5, TimeUnit.SECONDS);
                assertEquals(1L, old.sequence());
                assertEquals(new RealmJournal.Value(1, "1"), old.value());
                close.get(5, TimeUnit.SECONDS);
                assertThrows(IOException.class, () -> journal.readSnapshot("player:A"));
            } finally {
                finishRead.countDown();
            }
        }
        try (var reopened = RealmVolume.open(directory, 1, id)) {
            assertEquals(new RealmJournal.SnapshotValue(3, new RealmJournal.Value(2, "2")),
                    reopened.journal().readSnapshot("player:A"));
        }
    }

    @Test void aWriteThatBecameUnknownWhileReadingRejectsTheCapturedValue() throws Exception {
        RealmVolume.initialize(directory, 1, UUID.randomUUID());
        try (var options = new Options().setCreateIfMissing(false);
             RocksDB db = RocksDB.open(options, directory.toString());
             var workers = Executors.newFixedThreadPool(2)) {
            RealmJournal journal = new RealmJournal(db, (writes, batch) -> {
                db.write(writes, batch);
                throw new RocksDBException("lost write acknowledgement");
            });
            journal.recover();
            var captured = new CountDownLatch(1);
            var finishRead = new CountDownLatch(1);
            var slow = workers.submit(() -> journal.readSnapshot("player:A", (ignored, readOptions, key) -> {
                captured.countDown();
                await(finishRead);
                return db.get(readOptions, key);
            }));
            try {
                assertTrue(captured.await(5, TimeUnit.SECONDS));
                var failedWrite = workers.submit(() -> journal.commit(change("B", 0, "1")));
                var error = assertThrows(ExecutionException.class, () -> failedWrite.get(5, TimeUnit.SECONDS));
                assertInstanceOf(IOException.class, error.getCause());
            } finally {
                finishRead.countDown();
            }
            var error = assertThrows(ExecutionException.class, () -> slow.get(5, TimeUnit.SECONDS));
            assertInstanceOf(IOException.class, error.getCause());
        }
    }

    private static RealmJournal.Decision change(String owner, long version, String value) {
        return new RealmJournal.Decision(owner, "request-" + version, "digest", "UPDATE",
                value, value, List.of(new RealmJournal.Change("player:" + owner, version, value)));
    }

    private static void await(CountDownLatch latch) throws IOException {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) throw new IOException("Timed out waiting for snapshot test");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Snapshot test interrupted", e);
        }
    }
}
