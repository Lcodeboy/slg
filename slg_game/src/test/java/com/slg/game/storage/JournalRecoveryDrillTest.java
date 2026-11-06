package com.slg.game.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.rocksdb.Checkpoint;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

class JournalRecoveryDrillTest {
    @TempDir Path testRoot;
    final UUID volumeId = UUID.randomUUID();

    @ParameterizedTest
    @ValueSource(strings = {"6", "8"})
    void restoresANativeCheckpointThenReplaysOnlyIntoScratch(String format) throws Exception {
        Path root = testRoot.toRealPath();
        Path original = Files.createDirectory(root.resolve("realm"));
        Path snapshot = root.resolve("checkpoint"), secondary = Files.createDirectory(root.resolve("secondary"));
        RealmVolume.initialize(original, 1, volumeId);
        try (var opts = new Options().setCreateIfMissing(false); var primary = RocksDB.open(opts, original.toString())) {
            var journal = new RealmJournal(primary);
            journal.recover();
            journal.commit(change("1", 0));
            try (var checkpoint = Checkpoint.create(primary)) { checkpoint.createCheckpoint(snapshot.toString()); }
            journal.commit(change("2", 1));
        }
        // 在测试专有的两份数据上标注被观测格式。观察期间原卷和备份都只读，重放只能写进独立 scratch。
        if (format.equals("6")) {
            for (Path fixture : List.of(original, snapshot)) {
                try (var options = new Options(); var db = RocksDB.open(options, fixture.toString())) {
                    db.put("meta/format".getBytes(UTF_8), format.getBytes(UTF_8));
                }
            }
            assertThrows(IOException.class, () -> {
                try (var ignored = RealmVolume.open(original, 1, volumeId)) { fail("Expected write refusal"); }
            });
        }
        var sourceImage = image(snapshot);
        assertFalse(Files.exists(snapshot.resolve(".realm-volume-init")));
        try (var secondaryOptions = new Options().setMaxOpenFiles(-1).setDbLogDir(secondary.toString());
             var view = RocksDB.openAsSecondary(secondaryOptions, original.toString(), secondary.toString())) {
            view.tryCatchUpWithPrimary();
            var proof = JournalRecoveryDrill.verify(view, original, snapshot, 1, volumeId);
            assertEquals(new JournalRetentionPreview.RecoveryPoint(1, volumeId, 1, true), proof);
            var entry = RealmJournal.fixedEntry(view, 2);
            assertEquals("r2", entry.requestId());
            assertArrayEquals(format.getBytes(UTF_8), view.get("meta/format".getBytes(UTF_8)));
        }
        assertEquals(sourceImage, image(snapshot)); // 未给原快照补标记或改动 SST/WAL。
    }

    @Test void detectsWrongDataAtSnapshotTimeEvenWhenReplayWouldOverwriteIt() throws Exception {
        Path root = testRoot.toRealPath();
        Path original = Files.createDirectory(root.resolve("realm")), snapshot = root.resolve("checkpoint");
        RealmVolume.initialize(original, 1, volumeId);
        try (var opts = new Options().setCreateIfMissing(false); var db = RocksDB.open(opts, original.toString())) {
            var journal = new RealmJournal(db);
            journal.recover();
            journal.commit(change("1", 0));
            try (var checkpoint = Checkpoint.create(db)) { checkpoint.createCheckpoint(snapshot.toString()); }
            journal.commit(change("2", 1));
            // 只破坏用例中的独立 checkpoint，仍保留预期版本 1；回放完整覆盖型 change 后最终状态会一致。
            try (var corruptCopy = RocksDB.open(opts, snapshot.toString())) {
                byte[] bad = "{\"soldiers\":999}".getBytes(UTF_8);
                corruptCopy.put("Splayer:p".getBytes(UTF_8), ByteBuffer.allocate(8 + bad.length).putLong(1).put(bad).array());
            }
            var sourceImage = image(snapshot);
            assertThrows(IOException.class, () -> JournalRecoveryDrill.verify(db, original, snapshot, 1, volumeId));
            assertEquals(sourceImage, image(snapshot));
            assertEquals(2, journal.highSequence());
        }
    }

    @Test void rejectsWrongIdentityNestedPathsAliasesAndSymlinks() throws Exception {
        Path root = testRoot.toRealPath();
        Path original = Files.createDirectory(root.resolve("realm")), snapshot = root.resolve("checkpoint");
        RealmVolume.initialize(original, 1, volumeId);
        try (var opts = new Options().setCreateIfMissing(false); var db = RocksDB.open(opts, original.toString())) {
            var journal = new RealmJournal(db);
            journal.recover();
            journal.commit(change("1", 0));
            try (var checkpoint = Checkpoint.create(db)) { checkpoint.createCheckpoint(snapshot.toString()); }
            Path symlink = Files.createSymbolicLink(root.resolve("alias"), snapshot);
            Path nested = Files.createDirectory(original.resolve("nested"));
            assertThrows(IOException.class, () -> JournalRecoveryDrill.verify(db, original, original, 1, volumeId));
            assertThrows(IOException.class, () -> JournalRecoveryDrill.verify(db, original, nested, 1, volumeId));
            assertThrows(IOException.class, () -> JournalRecoveryDrill.verify(db, original, symlink, 1, volumeId));
            assertThrows(IOException.class, () -> JournalRecoveryDrill.verify(db, original, snapshot, 2, volumeId));
            assertThrows(IOException.class, () -> JournalRecoveryDrill.verify(db, original, snapshot, 1, UUID.randomUUID()));
            Path external = Files.writeString(root.resolve("must-remain"), "preserve");
            Files.createSymbolicLink(snapshot.resolve("outside"), external);
            assertThrows(IOException.class, () -> JournalRecoveryDrill.verify(db, original, snapshot, 1, volumeId));
            assertEquals("preserve", Files.readString(external));
        }
    }

    @Test void configuredScratchReserveCanStopTheDrillBeforeCopyingFiles() throws Exception {
        Path root = testRoot.toRealPath();
        Path original = Files.createDirectory(root.resolve("realm")), snapshot = root.resolve("checkpoint");
        RealmVolume.initialize(original, 1, volumeId);
        try (var opts = new Options().setCreateIfMissing(false); var db = RocksDB.open(opts, original.toString())) {
            try (var checkpoint = Checkpoint.create(db)) { checkpoint.createCheckpoint(snapshot.toString()); }
            var imageBefore = image(snapshot);
            // 只提高本次演练的预留参数模拟空间不足，既不填盘，也不变更运行区服的阈值。
            var failure = assertThrows(IOException.class,
                    () -> JournalRecoveryDrill.verify(db, original, snapshot, 1, volumeId, Long.MAX_VALUE));
            assertTrue(failure.getMessage().contains("Insufficient scratch space"));
            assertEquals(imageBefore, image(snapshot));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"7", "9"})
    void aPrunedOriginalCannotGenerateANewRecoveryProofFromIncompleteHistory(String format) throws Exception {
        Path root = testRoot.toRealPath();
        Path original = Files.createDirectory(root.resolve("realm")), snapshot = root.resolve("checkpoint");
        RealmVolume.initialize(original, 1, volumeId);
        try (var opts = new Options().setCreateIfMissing(false); var db = RocksDB.open(opts, original.toString())) {
            var journal = new RealmJournal(db);
            journal.recover();
            journal.commit(change("1", 0));
            try (var checkpoint = Checkpoint.create(db)) { checkpoint.createCheckpoint(snapshot.toString()); }
            journal.commit(change("2", 1));
            // 单测在这个 @TempDir 专属卷模拟清理后的磁盘布局。演练不能把缺失的历史当作账本空白。
            byte[] j1 = ByteBuffer.allocate(9).put((byte) 'J').putLong(1).array();
            try (var batch = new org.rocksdb.WriteBatch(); var writes = new org.rocksdb.WriteOptions()) {
                batch.delete(j1);
                batch.put("meta/format".getBytes(UTF_8), format.getBytes(UTF_8));
                batch.put("meta/journal-pruned-through".getBytes(UTF_8), ByteBuffer.allocate(8).putLong(1).array());
                db.write(writes, batch);
            }
            var checkpointImage = image(snapshot);
            var failure = assertThrows(IOException.class, () -> JournalRecoveryDrill.verify(db, original, snapshot, 1, volumeId));
            assertTrue(failure.getMessage().contains("pruned journal entries 1..1"));
            var report = RealmVolumePreview.observe(original, 1, volumeId, snapshot,
                    Map.of("SLG_PROJECTION_ENABLED", "false"));
            var preview = (JournalRetentionPreview.Result) report.get("journalRetention");
            assertEquals("BLOCKED", preview.state());
            assertEquals(1, preview.history().prunedThrough());
            assertEquals(2, preview.history().first());
            assertNull(preview.history().issue());
            assertTrue(report.get("snapshotDiagnostic").toString().contains("pruned journal entries 1..1"));
            assertEquals(checkpointImage, image(snapshot));
            assertNull(db.get(j1));
        }
    }

    private static RealmJournal.Decision change(String id, long version) {
        String task = """
                {"taskId":"t","playerId":"p","marchId":"m","type":"MARCH_ARRIVE",
                 "generation":1,"dueAt":500,"state":"PENDING"}
                """;
        return new RealmJournal.Decision("p", "r" + id, "digest", "DRILL_TEST", "{}", "{}",
                List.of(new RealmJournal.Change("player:p", version, "{\"soldiers\":" + id + "}"),
                        new RealmJournal.Change("task:t", version, task)));
    }

    private static Map<String, String> image(Path dir) throws Exception {
        var image = new TreeMap<String, String>();
        try (var files = Files.walk(dir)) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
                image.put(dir.relativize(file).toString(), HexFormat.of().formatHex(
                        MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file))));
            }
        }
        return image;
    }
}
