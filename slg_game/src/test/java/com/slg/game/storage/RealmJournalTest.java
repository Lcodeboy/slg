package com.slg.game.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class RealmJournalTest {
    private static final String DIGEST = "a".repeat(64);
    private static final String OTHER_DIGEST = "b".repeat(64);

    @TempDir Path directory;
    private final UUID volumeId = UUID.randomUUID();

    private RealmVolume create() throws Exception {
        Path path = Files.createDirectory(directory.resolve("realm"));
        RealmVolume.initialize(path, 1, volumeId);
        return RealmVolume.open(path, 1, volumeId);
    }

    @Test void projectionPagesAreContiguousAndDoNotSkipHolesBelowTheWatermark() throws Exception {
        Path path = directory.resolve("realm");
        try (RealmVolume volume = create()) {
            var journal = volume.journal();
            for (String account : List.of("A", "B", "C")) journal.commit(initial(account));
            assertEquals(List.of(journal.entry(1), journal.entry(2)), journal.entriesAfter(0, 2));
            assertEquals(List.of(journal.entry(3)), journal.entriesAfter(2, 2));
            assertTrue(journal.entriesAfter(3, 2).isEmpty());
            assertThrows(RealmJournal.InvalidPrefix.class, () -> journal.entriesAfter(4, 1));
        }
        try (var options = new Options(); var db = RocksDB.open(options, path.toString())) {
            db.delete(ByteBuffer.allocate(9).put((byte)'J').putLong(2).array());
        }
        try (var reopened = RealmVolume.open(path, 1, volumeId)) {
            var journal = reopened.journal();
            assertEquals(3, journal.highSequence());
            assertThrows(RealmJournal.InvalidPrefix.class, () -> journal.entriesAfter(1, 10));
        }
    }

    @Test void concurrentClientWritesShareOneLagBudgetButReplaysAndSystemWorkStillPass() throws Exception {
        try (var volume = create(); var threads = Executors.newFixedThreadPool(2)) {
            var journal = volume.journal();
            var start = new CountDownLatch(1);
            var attempts = new ArrayList<Future<String>>();
            for (String account : List.of("A", "B")) {
                attempts.add(threads.submit(() -> {
                    start.await();
                    try {
                        journal.commitWithOutcome(initial(account), high -> high < 1);
                        return account;
                    } catch (RealmJournal.WriteDeferred limited) {
                        return "";
                    }
                }));
            }
            start.countDown();
            String winner = attempts.getFirst().get(5, TimeUnit.SECONDS) + attempts.getLast().get(5, TimeUnit.SECONDS);
            assertEquals(1, winner.length());
            assertEquals(1, journal.highSequence());
            assertFalse(journal.commitWithOutcome(initial(winner), high -> false).applied());
            assertEquals(2, journal.commit(initial("system")).sequence());
        }
    }

    @Test void aLostWriteAcknowledgementStopsFurtherWorkUntilTheOriginalVolumeIsReopened() throws Exception {
        Path path = Files.createDirectory(directory.resolve("realm"));
        RealmVolume.initialize(path, 1, volumeId);
        var first = initial("A");
        var next = initial("B");
        try (Options options = new Options().setCreateIfMissing(false);
             RocksDB db = RocksDB.open(options, path.toString())) {
            var journal = new RealmJournal(db, (writes, batch) -> {
                db.write(writes, batch);
                throw new RocksDBException("simulated acknowledgement lost after native write");
            });
            journal.recover();
            assertThrows(IOException.class, () -> journal.commit(first));
            assertThrows(IOException.class, () -> journal.read("player:A"));
            assertThrows(IOException.class, journal::highSequence);
            assertThrows(IOException.class, () -> journal.lookup(first.scope(), first.requestId(), DIGEST));
            assertThrows(IOException.class, () -> journal.findReceipt(first.scope(), first.requestId()));
            assertThrows(IOException.class, () -> journal.commit(next));
        }
        try (var reopened = RealmVolume.open(path, 1, volumeId)) {
            var journal = reopened.journal();
            assertEquals(1L, journal.highSequence());
            assertEquals(1L, journal.read("player:A").version());
            assertNull(journal.read("player:B"));
            var original = journal.lookup(first.scope(), first.requestId(), DIGEST);
            assertEquals(1L, original.sequence());
            assertEquals(original, journal.findReceipt(first.scope(), first.requestId()));
            assertEquals(original, journal.commit(first));
            assertEquals(2L, journal.commit(next).sequence());
        }
    }

    @Test void anUnreadableReceiptOrOneBeyondTheCommittedWatermarkCannotLookLikeAnAbsentRequest() throws Exception {
        var command = initial("A");
        try (var volume = create()) {
            var journal = volume.journal();
            assertNull(journal.findReceipt(command.scope(), command.requestId()));
            journal.commit(command);
        }
        // 只污染此用例的新卷。已提交的 J1 和水位保持合法，以便单独验证 R 记录的读取边界。
        try (var options = new Options(); var db = RocksDB.open(options, directory.resolve("realm").toString())) {
            byte[] key;
            try (var iterator = db.newIterator()) {
                iterator.seek(new byte[]{'R'});
                assertTrue(iterator.isValid());
                key = iterator.key();
                assertEquals('R', key[0]);
            }
            var journal = new RealmJournal(db);
            journal.recover();
            for (String invalid : List.of("broken", "{\"sequence\":2,\"digest\":\"abc\",\"resultJson\":\"{}\"}")) {
                db.put(key, invalid.getBytes(StandardCharsets.UTF_8));
                assertThrows(IOException.class, () -> journal.findReceipt(command.scope(), command.requestId()));
                assertEquals(1, journal.highSequence());
            }
        }
    }

    private static RealmJournal.Decision initial(String owner) {
        return new RealmJournal.Decision("player:" + owner, "first", DIGEST, "PLAYER_CREATED",
                "{\"owner\":\"" + owner + "\"}", "{\"created\":true}",
                List.of(new RealmJournal.Change("player:" + owner, 0, "{\"soldiers\":12}")));
    }

    @Test void concurrentRetriesReturnOneReceiptPerOwnerAndFormAGapFreeLog() throws Exception {
        try (RealmVolume volume = create(); var workers = Executors.newFixedThreadPool(8)) {
            RealmJournal journal = volume.journal();
            assertEquals(0L, journal.highSequence());
            CountDownLatch start = new CountDownLatch(1);
            List<Future<RealmJournal.Receipt>> pending = new ArrayList<>();
            List<String> owners = new ArrayList<>();
            // 多个账号复用同一请求 ID；每一个账号同时投递两次。
            for (int i = 0; i < 32; i++) {
                String owner = "A" + i / 2;
                owners.add(owner);
                pending.add(workers.submit(() -> {
                    assertTrue(start.await(10, TimeUnit.SECONDS));
                    return journal.commit(initial(owner));
                }));
            }
            start.countDown();
            Map<String, RealmJournal.Receipt> receipts = new HashMap<>();
            for (int i = 0; i < pending.size(); i++) {
                RealmJournal.Receipt received = pending.get(i).get(15, TimeUnit.SECONDS);
                RealmJournal.Receipt previous = receipts.put(owners.get(i), received);
                if (previous != null) assertEquals(previous, received);
            }
            assertEquals(16, receipts.size());
            assertEquals(receipts.size(), journal.highSequence());
            Set<Long> sequences = new HashSet<>();
            for (var pair : receipts.entrySet()) {
                long seq = pair.getValue().sequence();
                assertTrue(sequences.add(seq));
                assertEquals(seq, journal.entry(seq).sequence());
                assertEquals(initial(pair.getKey()).changes(), journal.entry(seq).changes());
                assertEquals(pair.getValue(), journal.lookup("player:" + pair.getKey(), "first", DIGEST));
                assertEquals(1L, journal.read("player:" + pair.getKey()).version());
            }
            for (long seq = 1; seq <= 16; seq++) assertTrue(sequences.contains(seq));
            assertNull(journal.entry(17));
        }
    }

    @Test void staleMultiObjectCommandCannotLeavePartialStateOrAdvanceTheCursor() throws Exception {
        try (RealmVolume volume = create()) {
            var journal = volume.journal();
            var first = initial("A");
            var receipt = journal.commit(first);
            List<RealmJournal.Change> invalid = List.of(
                    new RealmJournal.Change("task:t1", 0, """
                            {"taskId":"t1","playerId":"A","marchId":"m1","type":"MARCH_ARRIVE",
                             "generation":1,"dueAt":500,"state":"PENDING"}
                            """),
                    new RealmJournal.Change("player:A", 0, "{\"soldiers\":0}"));
            var attempt = new RealmJournal.Decision("player:A", "march1", OTHER_DIGEST, "MARCH_STARTED",
                    "{}", "{}", invalid);
            assertThrows(RealmJournal.Stale.class, () -> journal.commit(attempt));
            assertNull(journal.read("task:t1"));
            assertTrue(journal.dueTasks(500, null, 1).isEmpty());
            assertNull(journal.lookup("player:A", "march1", OTHER_DIGEST));
            assertEquals(1L, journal.read("player:A").version());
            assertEquals(first.changes().getFirst().json(), journal.read("player:A").json());
            assertEquals(1L, journal.highSequence());
            assertNull(journal.entry(2));

            // 同 ID、同摘要的重试先读回执，不能被已过时的版本条件挡住。
            assertEquals(receipt, journal.commit(first));
            assertThrows(RealmJournal.Conflict.class, () -> journal.lookup("player:A", "first", OTHER_DIGEST));
            assertThrows(RealmJournal.Conflict.class, () -> journal.commit(
                    new RealmJournal.Decision("player:A", "first", OTHER_DIGEST, "OTHER", "{}", "{}", invalid)));

            var valid = List.of(invalid.getFirst(), new RealmJournal.Change("player:A", 1, "{\"soldiers\":0}"));
            var result = journal.commit(new RealmJournal.Decision("player:A", "march1", OTHER_DIGEST,
                    "MARCH_STARTED", "{}", "{}", valid));
            assertEquals(2L, result.sequence());
            assertEquals(valid, journal.entry(2).changes());
        }
        try (var reopened = RealmVolume.open(directory.resolve("realm"), 1, volumeId)) {
            var journal = reopened.journal();
            assertEquals(2L, journal.highSequence());
            assertEquals(2L, journal.read("player:A").version());
            assertEquals(1L, journal.read("task:t1").version());
            assertEquals(2L, journal.lookup("player:A", "march1", OTHER_DIGEST).sequence());
        }
    }
}
