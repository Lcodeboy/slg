package com.slg.game.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** 所有实际删除或损坏注入都只对本用例新建的临时卷执行。 */
class RealmPrunedJournalTest {
    @TempDir Path directory;
    private final UUID volumeId = UUID.randomUUID();
    private static final byte[] FORMAT = bytes("meta/format");
    private static final byte[] FLOOR = bytes(RealmJournal.PRUNED_THROUGH_KEY);

    @Test void openingAndPagingAValidRetainedSuffixPreservesHistoricalReceipts() throws Exception {
        prepare();
        try (var options = new Options(); var db = RocksDB.open(options, directory.toString())) {
            trimTestPrefix(db, 2);
            assertNull(db.get(RealmJournal.journalKey(1)));
            assertNull(db.get(RealmJournal.journalKey(2)));
        }
        try (var volume = RealmVolume.open(directory, 1, volumeId)) {
            var journal = volume.journal();
            assertEquals(5, journal.highSequence());
            var first = journal.inspectPage(0, 1);
            assertEquals(new RealmJournal.Inspection(5, 3L, 5L, 3, null, 2), first);
            assertEquals(first, journal.inspectPage(2, 1)); // F 没有日志键，不能 seek JF 当分页锚点。
            assertEquals(new RealmJournal.Inspection(5, 3L, 5L, 5, null, 2), journal.inspectPage(3, 5));
            assertThrows(RealmJournal.InvalidPrefix.class, () -> journal.entry(1));
            assertThrows(RealmJournal.InvalidPrefix.class, () -> journal.entry(2));
            assertThrows(RealmJournal.InvalidPrefix.class, () -> journal.entriesAfter(0, 2));
            assertThrows(RealmJournal.InvalidPrefix.class, () -> journal.entriesAfter(1, 2));
            assertEquals(List.of(journal.entry(3), journal.entry(4)), journal.entriesAfter(2, 2));
            assertEquals(List.of(journal.entry(5)), journal.entriesAfter(4, 2));
            assertTrue(journal.entriesAfter(5, 2).isEmpty());
            var receipt = journal.findReceipt("p", "r1");
            assertEquals(1, receipt.sequence());
            assertEquals(decision(1).resultJson(), receipt.resultJson());
            assertNull(journal.findReceipt("another-owner", "r1"));
            assertEquals(receipt, journal.lookup("p", "r1", "digest"));
            assertEquals(receipt, journal.commit(decision(1)));
            assertEquals(5, journal.highSequence());
            assertEquals(5, journal.read("player:p").version());
            assertEquals(6, journal.commit(decision(6)).sequence());
        }
        try (var volume = RealmVolume.open(directory, 1, volumeId)) {
            assertEquals(new RealmJournal.Inspection(6, 3L, 6L, 6, null, 2), volume.journal().inspectPage(0, 8));
        }
    }

    @Test void formatAndPruneMarkerAreAnInseparableDeclarationOfAnAlreadyAppliedPrefix() throws Exception {
        prepare();
        try (var options = new Options(); var db = RocksDB.open(options, directory.toString())) {
            trimTestPrefix(db, 2);
            for (byte[] bad : List.of(new byte[0], new byte[]{2}, number(-1), number(0), number(5), number(6))) {
                db.put(FLOOR, bad);
                assertCannotRecover(db);
            }
            db.delete(FLOOR);
            assertCannotRecover(db);
            db.put(FLOOR, number(2));
            db.put(FORMAT, bytes("8"));
            assertCannotRecover(db);
            db.delete(FLOOR);
            assertCannotRecover(db); // 未登记 F 的完整日志卷也不能将丢失的头两条解释成一次清理。
        }
    }

    @Test void recoveryRejectsAnInconsistentFloorAndAHoleOrBadEntryInsideTheRetainedSuffix() throws Exception {
        prepare();
        try (var options = new Options(); var db = RocksDB.open(options, directory.toString());
             var reads = new ReadOptions()) {
            byte[] originalJ2 = db.get(RealmJournal.journalKey(2));
            byte[] originalJ4 = db.get(RealmJournal.journalKey(4));
            trimTestPrefix(db, 2);
            db.put(RealmJournal.journalKey(2), originalJ2); // F 声称 2 已清，但物理首键却是 2。
            assertCannotRecover(db);
            db.delete(RealmJournal.journalKey(2));
            db.put(FLOOR, number(1)); // F 声称 2 仍在，物理首键却是 3。
            assertCannotRecover(db);
            db.put(FLOOR, number(2));
            db.delete(RealmJournal.journalKey(4));
            var hole = RealmJournal.inspectPage(db, reads, 0, 4);
            assertEquals(2, hole.prunedThrough());
            assertEquals(3, hole.verifiedThrough());
            assertNotNull(hole.issue());
            assertCannotRecover(db);
            db.put(RealmJournal.journalKey(4), bytes("corrupt"));
            assertCannotRecover(db);
            db.put(RealmJournal.journalKey(4), originalJ4);
            db.delete(RealmJournal.journalKey(5));
            assertCannotRecover(db);
        }
    }

    private void prepare() throws IOException {
        RealmVolume.initialize(directory, 1, volumeId);
        try (var volume = RealmVolume.open(directory, 1, volumeId)) {
            for (int n = 1; n <= 5; n++) volume.journal().commit(decision(n));
        }
    }

    private static void assertCannotRecover(RocksDB db) {
        var journal = new RealmJournal(db);
        assertThrows(IOException.class, journal::recover);
        assertThrows(IOException.class, journal::highSequence);
    }

    /** 按实际维护提交的形状制造测试夹具。这里不提供可以指定目录的删除工具或对外入口。 */
    private static void trimTestPrefix(RocksDB db, long through) throws Exception {
        try (var batch = new WriteBatch(); var writes = new WriteOptions().setDisableWAL(false).setSync(true)) {
            batch.deleteRange(RealmJournal.journalKey(1), RealmJournal.journalKey(through + 1));
            batch.put(FORMAT, bytes("9"));
            batch.put(FLOOR, number(through));
            db.write(writes, batch);
        }
    }

    private static RealmJournal.Decision decision(int n) {
        return new RealmJournal.Decision("p", "r" + n, "digest", "TEST", "{}", "{}",
                List.of(new RealmJournal.Change("player:p", n - 1L, "{\"balance\":" + n + "}")));
    }

    private static byte[] number(long n) { return ByteBuffer.allocate(Long.BYTES).putLong(n).array(); }
    private static byte[] bytes(String text) { return text.getBytes(StandardCharsets.US_ASCII); }
}
