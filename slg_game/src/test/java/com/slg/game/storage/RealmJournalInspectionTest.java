package com.slg.game.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;

import static org.junit.jupiter.api.Assertions.*;

class RealmJournalInspectionTest {
    @TempDir Path path;
    final UUID volumeId = UUID.randomUUID();

    @Test void boundedPagesKeepTheirContiguousPrefixAcrossNewCommits() throws Exception {
        RealmVolume.initialize(path, 1, volumeId);
        try (var volume = RealmVolume.open(path, 1, volumeId)) {
            var journal = volume.journal();
            assertEquals(new RealmJournal.Inspection(0, null, null, 0, null), journal.inspectPage(0, 2));
            for (int i = 1; i <= 4; i++) journal.commit(decision(i, "\"ok\""));
            assertEquals(new RealmJournal.Inspection(4, 1L, 4L, 2, null), journal.inspectPage(0, 2));
            journal.commit(decision(5, "\"ok\""));
            assertEquals(new RealmJournal.Inspection(5, 1L, 5L, 4, null), journal.inspectPage(2, 2));
            assertEquals(new RealmJournal.Inspection(5, 1L, 5L, 5, null), journal.inspectPage(4, 2));
            assertEquals(journal.inspectPage(4, 2), journal.inspectPage(5, 2));
            assertEquals(5, journal.highSequence());
            assertEquals(5, journal.entry(5).sequence());
            assertThrows(IllegalArgumentException.class, () -> journal.inspectPage(-1, 2));
            assertThrows(IllegalArgumentException.class, () -> journal.inspectPage(0, 129));
        }
    }

    @Test void encodedBytesBoundEachPageEvenWhenEntryLimitAllowsTheWholeJournal() throws Exception {
        RealmVolume.initialize(path, 1, volumeId);
        try (var volume = RealmVolume.open(path, 1, volumeId)) {
            String payload = "\"" + "x".repeat(63 * 1024) + "\"";
            for (int n = 1; n <= 40; n++) volume.journal().commit(decision(n, payload));
            var page = volume.journal().inspectPage(0, 128);
            assertNull(page.issue());
            assertTrue(page.verifiedThrough() > 0 && page.verifiedThrough() < page.high());
            var rest = volume.journal().inspectPage(page.verifiedThrough(), 128);
            assertEquals(new RealmJournal.Inspection(40, 1L, 40L, 40, null), rest);
        }
    }

    @Test void aMissingOrUnreadableMiddleEntryStopsAtItsPredecessor() throws Exception {
        createThreeEntries();
        try (var opt = new Options(); var db = RocksDB.open(opt, path.toString()); var read = new ReadOptions()) {
            db.delete(key(2)); // 只在本用例创建的临时卷模拟损坏，不存在生产删除路径。
            var gap = RealmJournal.inspectPage(db, read, 1, 4);
            assertEquals(1, gap.verifiedThrough());
            assertTrue(gap.issue().contains("sequence 2"));
            db.put(key(2), "malformed".getBytes(StandardCharsets.UTF_8));
            var bad = RealmJournal.inspectPage(db, read, 0, 4);
            assertEquals(1, bad.verifiedThrough());
            assertTrue(bad.issue().contains("Invalid realm journal entry"));
            assertEquals(1L, bad.first());
            assertEquals(3L, bad.last());
        }
    }

    @Test void malformedKeyBetweenPagesCannotBeSkippedByTheNextSeek() throws Exception {
        createThreeEntries();
        try (var opt = new Options(); var db = RocksDB.open(opt, path.toString()); var read = new ReadOptions()) {
            byte[] malformed = Arrays.copyOf(key(1), 10); // 1 < 畸形键 < 2，正好落在页边界。
            db.put(malformed, new byte[]{1});
            var firstPage = RealmJournal.inspectPage(db, read, 0, 1);
            assertEquals(1, firstPage.verifiedThrough());
            assertNull(firstPage.issue());
            var secondPage = RealmJournal.inspectPage(db, read, firstPage.verifiedThrough(), 1);
            assertEquals(1, secondPage.verifiedThrough());
            assertTrue(secondPage.issue().contains("sequence 2"));
        }
    }

    @Test void malformedBoundaryMetadataAndMovedWatermarkAreNeverCertified() throws Exception {
        createThreeEntries();
        try (var opt = new Options(); var db = RocksDB.open(opt, path.toString()); var read = new ReadOptions()) {
            assertNotNull(RealmJournal.inspectPage(db, read, 4, 4).issue());
            db.put(key(4), new byte[]{1});
            var tail = RealmJournal.inspectPage(db, read, 0, 4);
            assertEquals(4L, tail.last());
            assertNotNull(tail.issue()); // 已提交水位是 3，不能把物理尾键 4 视为第 4 次提交。
            db.delete(key(4));
            db.put(new byte[]{'J'}, new byte[]{1});
            assertNotNull(RealmJournal.inspectPage(db, read, 0, 4).issue());
            db.delete(new byte[]{'J'});
            db.delete(key(1));
            var missingFirst = RealmJournal.inspectPage(db, read, 0, 4);
            assertEquals(2L, missingFirst.first());
            assertNotNull(missingFirst.issue());
            db.put("meta/journal-high".getBytes(StandardCharsets.UTF_8), new byte[]{0});
            assertThrows(IOException.class, () -> RealmJournal.inspectPage(db, read, 0, 4));
        }
    }

    private void createThreeEntries() throws IOException {
        RealmVolume.initialize(path, 1, volumeId);
        try (var volume = RealmVolume.open(path, 1, volumeId)) {
            for (int n = 1; n <= 3; n++) volume.journal().commit(decision(n, "{}"));
        }
    }

    private static RealmJournal.Decision decision(int n, String payload) {
        return new RealmJournal.Decision("inspection", "request-" + n, "digest", "INSPECTION_TEST",
                payload, "{}", List.of());
    }

    private static byte[] key(long n) {
        return ByteBuffer.allocate(9).put((byte) 'J').putLong(n).array();
    }
}
