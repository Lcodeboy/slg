package com.slg.game.storage;

import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.UUID;

import static java.nio.file.FileVisitResult.CONTINUE;

/** 只在显式指定独立 checkpoint 时运行。演练的全部写入、恢复和回收都发生在自己创建的临时目录。 */
public final class JournalRecoveryDrill {
    private static final long MINIMUM_RESERVE = 2L << 30;
    private JournalRecoveryDrill() {}

    /**
     * fixedOriginal 必须是调用者已冻结的同一原卷视图。从头重放到快照水位 S 可发现快照中丢失的
     * 回执、任务索引或错误的旧对象，即使它们随后会被另一条日志覆盖。再把检查通过的副本推进到 H，
     * 对比全部逻辑键值，证明这个 S 是可恢复、可接续的位置。返回的证据不授权任何删除。
     */
    public static JournalRetentionPreview.RecoveryPoint verify(RocksDB fixedOriginal, Path originalDirectory,
            Path checkpoint, int realmId, UUID volumeId) throws IOException {
        return verify(fixedOriginal, originalDirectory, checkpoint, realmId, volumeId, MINIMUM_RESERVE);
    }

    /** scratch 必须同时满足本次配置的低磁盘告警预留，以及演练自身至少 2 GiB 的预留。 */
    public static JournalRetentionPreview.RecoveryPoint verify(RocksDB fixedOriginal, Path originalDirectory,
            Path checkpoint, int realmId, UUID volumeId, long reserveBytes) throws IOException {
        if (reserveBytes < 0) throw new IOException("Invalid scratch space reserve");
        Path original = canonicalDirectory(originalDirectory), snapshot = canonicalDirectory(checkpoint);
        if (snapshot.startsWith(original) || original.startsWith(snapshot) || Files.isSameFile(snapshot, original))
            throw new IOException("Checkpoint must be independent of the original directory");
        Path temp = Path.of(System.getProperty("java.io.tmpdir")).toRealPath();
        if (temp.startsWith(original) || temp.startsWith(snapshot))
            throw new IOException("Scratch directory would write into the original or checkpoint");

        try {
            verifyIdentity(fixedOriginal, realmId, volumeId);
            long high = watermark(fixedOriginal);
            // 先计量源文件和编码日志，再允许开始复制。倍数留给两个复本、状态/回执/WAL 以及压缩；
            // 这是保守估计，不能代替对实际剩余空间的复核。预算不足宁可没有恢复点。
            long checkpointBytes = visitCheckpoint(snapshot, null, null);
            long journalBytes = journalBytes(fixedOriginal, high);
            long estimate = Math.addExact(Math.multiplyExact(2L, checkpointBytes), Math.multiplyExact(16L, journalBytes));
            var space = new SpaceBudget(Files.getFileStore(temp), Math.max(MINIMUM_RESERVE, reserveBytes));
            space.require(estimate);
            try (Scratch scratch = new Scratch(Files.createTempDirectory(temp, "slg-recovery-drill-"))) {
                Path copy = Files.createDirectory(scratch.path().resolve("restore"));
                if (visitCheckpoint(snapshot, copy, space) != checkpointBytes)
                    throw new IOException("Checkpoint files changed during the restore drill");

                // Native checkpoint 不包含应用自建的空初始化标记。先在副本中核对数据库里真正的身份，
                // 再仅为副本补标记，随后在私有副本验证身份、日志和状态；源 checkpoint 始终只读。
                long snapshotSeq;
                try (var options = new Options().setCreateIfMissing(false);
                     var restored = RocksDB.openReadOnly(options, copy.toString())) {
                    verifyIdentity(restored, realmId, volumeId);
                    snapshotSeq = watermark(restored);
                }
                if (snapshotSeq > high) throw new IOException("Checkpoint is ahead of the original view");
                Path marker = copy.resolve(".realm-volume-init");
                if (!Files.exists(marker, LinkOption.NOFOLLOW_LINKS)) Files.createFile(marker);

                // 所有历史仍在原卷：从 0 重放到 S，先验证 checkpoint 在 S 就是正确的状态。
                Path reference = Files.createDirectory(scratch.path().resolve("reference"));
                RealmVolume.initialize(reference, realmId, volumeId, RealmVolumePreview.verifyRetentionFormat(fixedOriginal));
                try (var rebuilt = RealmVolume.openRecoveryCopy(reference, realmId, volumeId)) {
                    replay(fixedOriginal, rebuilt.journal(), 0, snapshotSeq, space);
                }
                try (var options = new Options().setCreateIfMissing(false);
                     var expected = RocksDB.openReadOnly(options, reference.toString())) {
                    compareAll(expected, copy);
                }

                try (var restored = RealmVolume.openRecoveryCopy(copy, realmId, volumeId)) {
                    replay(fixedOriginal, restored.journal(), snapshotSeq, high, space);
                }
                compareAll(fixedOriginal, copy);
                return new JournalRetentionPreview.RecoveryPoint(realmId, volumeId, snapshotSeq, true);
            }
        } catch (RocksDBException e) {
            throw new IOException("Checkpoint could not be restored and checked in scratch", e);
        } catch (RuntimeException e) {
            throw new IOException("Invalid checkpoint or insufficient restore budget", e);
        }
    }

    private static Path canonicalDirectory(Path directory) throws IOException {
        Path absolute = directory.toAbsolutePath();
        Path real = absolute.toRealPath();
        // 连父目录也要核对；不能通过 symlink/.. 把源或快照映射进 scratch。
        if (!absolute.equals(real) || !Files.isDirectory(real, LinkOption.NOFOLLOW_LINKS))
            throw new IOException("Expected a canonical directory without links or aliases");
        return real;
    }

    private static void verifyIdentity(RocksDB db, int realmId, UUID volumeId) throws IOException, RocksDBException {
        RealmVolumePreview.verifyMetadata(db, "meta/realm", Integer.toString(realmId));
        RealmVolumePreview.verifyMetadata(db, "meta/volume", volumeId.toString());
        RealmVolumePreview.verifyRetentionFormat(db);
        try (var reads = new ReadOptions().setFillCache(false)) {
            long floor = RealmJournal.readPrunedThrough(db, reads, watermark(db));
            // 此验证器用原始事实从 0 重建快照在 S 的正确状态，再回放到 H。清理过的原卷
            // 已缺 1..F；仅凭剩余日志，不能证明另一个快照在 S 就正确。停止发放新的恢复凭据，
            // 不把 F 当作新的“空卷起点”；后续若要继续清理，需要另外一条可验证的历史证明链。
            if (floor > 0) throw new IOException("Cannot verify a new recovery point without pruned journal entries 1.." + floor);
        }
    }

    private static long watermark(RocksDB db) throws IOException, RocksDBException {
        byte[] raw = db.get("meta/journal-high".getBytes(StandardCharsets.US_ASCII));
        if (raw == null || raw.length != Long.BYTES || ByteBuffer.wrap(raw).getLong() < 0)
            throw new IOException("Missing or malformed journal watermark during restore");
        return ByteBuffer.wrap(raw).getLong();
    }

    private static long journalBytes(RocksDB original, long high) throws IOException, RocksDBException {
        try (var reads = new ReadOptions().setFillCache(false); RocksIterator it = original.newIterator(reads)) {
            long bytes = 0, sequence = 0;
            byte[] key = new byte[9], empty = new byte[0];
            for (it.seek(new byte[]{'J'}); it.isValid(); it.next()) {
                int keySize = it.key(key);
                if (keySize == 0 || key[0] != 'J') break;
                if (sequence == high || keySize != 9 || ByteBuffer.wrap(key, 1, 8).getLong() != ++sequence)
                    throw new IOException("Original journal contains a gap or an extra key");
                int size = it.value(empty);
                if (size <= 0 || size > RealmJournal.MAX_ENTRY_BYTES)
                    throw new IOException("Invalid original journal size at " + sequence);
                bytes = Math.addExact(bytes, size);
            }
            it.status();
            if (sequence != high) throw new IOException("Original journal ends before the recorded watermark");
            return bytes;
        }
    }

    private static long visitCheckpoint(Path source, Path copy, SpaceBudget space) throws IOException {
        class Visitor extends SimpleFileVisitor<Path> {
            long bytes;
            @Override public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                if (copy != null && !dir.equals(source)) Files.createDirectory(copy.resolve(source.relativize(dir)));
                return CONTINUE;
            }
            @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                // Native checkpoint 可与原卷硬链接不可变 SST；逐字节复制成独立 inode，禁止 follow 符号链接。
                if (!attrs.isRegularFile()) throw new IOException("Checkpoint includes a link or special file");
                bytes = Math.addExact(bytes, attrs.size());
                if (copy != null) copyFile(file, copy.resolve(source.relativize(file)), attrs.size(), space);
                return CONTINUE;
            }
        }
        var visitor = new Visitor();
        Files.walkFileTree(source, visitor);
        if (copy != null) {
            Path current = copy.resolve("CURRENT");
            if (!Files.isRegularFile(current, LinkOption.NOFOLLOW_LINKS) || Files.size(current) > 128
                    || !Files.readString(current, StandardCharsets.US_ASCII).matches("MANIFEST-[0-9]+\\n"))
                throw new IOException("Invalid checkpoint manifest pointer");
        }
        return visitor.bytes;
    }

    private static void copyFile(Path source, Path target, long expectedSize, SpaceBudget space) throws IOException {
        space.require(expectedSize);
        try (InputStream input = Files.newInputStream(source, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
             OutputStream output = Files.newOutputStream(target, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            byte[] chunk = new byte[64 * 1024];
            long copied = 0, nextCheck = 64L << 20;
            int count;
            while ((count = input.read(chunk)) != -1) {
                copied = Math.addExact(copied, count);
                if (copied > expectedSize) throw new IOException("Checkpoint grew during copy");
                if (copied >= nextCheck) {
                    space.require(expectedSize - copied + count);
                    nextCheck = copied + (64L << 20);
                }
                output.write(chunk, 0, count);
            }
            if (copied != expectedSize) throw new IOException("Checkpoint shrank during copy");
        }
    }

    private record SpaceBudget(FileStore filesystem, long reserveBytes) {
        void require(long plannedBytes) throws IOException {
            // 用减法比较以免超大 reserve + 预计空间溢出，之后的每次复查都使用同一预留值。
            if (filesystem.isReadOnly() || filesystem.getUsableSpace() - reserveBytes < plannedBytes)
                throw new IOException("Insufficient scratch space for checkpoint restore and replay");
        }
    }

    private static void replay(RocksDB source, RealmJournal target, long after, long through, SpaceBudget space)
            throws IOException {
        if (target.highSequence() != after) throw new IOException("Restored journal does not start at expected sequence");
        long sequence = after;
        space.require(0);
        while (sequence < through) {
            if ((sequence - after) % 64 == 0) space.require(0);
            RealmJournal.Entry entry = RealmJournal.fixedEntry(source, ++sequence);
            // 使用日志中原已决定的快照、预期版本和原回执；严禁按当前规则重新运行派兵/战斗/发奖。
            var decision = new RealmJournal.Decision(entry.scope(), entry.requestId(), entry.digest(), entry.factType(),
                    entry.factJson(), entry.resultJson(), entry.changes());
            var outcome = target.commitWithOutcome(decision);
            if (!outcome.applied() || outcome.receipt().sequence() != sequence)
                throw new IOException("Checkpoint replay disagrees at sequence " + sequence);
        }
    }

    /** 比较键值而非引擎文件。SST 压缩布局可以不同，所有状态、回执、索引、业务日志及元数据必须一致。 */
    private static void compareAll(RocksDB expected, Path restored) throws RocksDBException, IOException {
        try (var options = new Options().setCreateIfMissing(false);
             var actual = RocksDB.openReadOnly(options, restored.toString());
             var reads = new ReadOptions().setFillCache(false);
             var left = expected.newIterator(reads); var right = actual.newIterator(reads)) {
            left.seekToFirst(); right.seekToFirst();
            byte[] keyA = new byte[1024], keyB = new byte[1024];
            byte[] valueA = new byte[RealmJournal.MAX_ENTRY_BYTES], valueB = new byte[RealmJournal.MAX_ENTRY_BYTES];
            while (left.isValid() && right.isValid()) {
                int keySize = left.key(keyA), valueSize = left.value(valueA);
                if (keySize > keyA.length || keySize != right.key(keyB)
                        || !Arrays.equals(keyA, 0, keySize, keyB, 0, keySize)
                        || valueSize > valueA.length || valueSize != right.value(valueB)
                        || !Arrays.equals(valueA, 0, valueSize, valueB, 0, valueSize))
                    throw new IOException("Restored logical keys or values differ from the reference");
                left.next(); right.next();
            }
            left.status(); right.status();
            if (left.isValid() || right.isValid()) throw new IOException("Restored logical key count differs");
        }
    }

    private record Scratch(Path path) implements AutoCloseable {
        @Override public void close() throws IOException {
            // 此 path 只由 createTempDirectory 返回；walkFileTree 默认不跟随链接，绝不回收输入目录。
            Files.walkFileTree(path, new SimpleFileVisitor<>() {
                @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return CONTINUE;
                }
                @Override public FileVisitResult postVisitDirectory(Path dir, IOException error) throws IOException {
                    if (error != null) throw error;
                    Files.delete(dir);
                    return CONTINUE;
                }
            });
        }
    }
}
