package com.slg.game.storage;

import com.slg.game.projection.RetentionCursorProbe;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/** 在线原卷的一次性只读预演。故意与持有写句柄的恢复和初始化路径分离。 */
public final class RealmVolumePreview {
    private RealmVolumePreview() {}

    /**
     * secondary 的运行文件只写进系统临时目录。追赶一次后冻结视图：后续所有页和 SQL 游标锚点
     * 都与这一个原卷水位比较。两端在取证过程中变化则报告未确认，操作者可以重新预演。
     */
    public static Map<String, Object> observe(Path directory, int realmId, UUID volumeId,
                                               Path checkpoint, Map<String, String> env) throws IOException {
        Path primary = directory.toRealPath();
        if (!directory.toAbsolutePath().equals(primary) || !Files.isRegularFile(primary.resolve(".realm-volume-init"), LinkOption.NOFOLLOW_LINKS)
                || !Files.isRegularFile(primary.resolve("CURRENT"), LinkOption.NOFOLLOW_LINKS))
            throw new IOException("Preview requires an existing initialized realm volume");
        Path tempRoot = Path.of(System.getProperty("java.io.tmpdir")).toRealPath();
        if (tempRoot.startsWith(primary) || (checkpoint != null && Files.exists(checkpoint) && tempRoot.startsWith(checkpoint.toRealPath())))
            throw new IOException("Preview scratch must be outside the original volume and snapshot");
        long keep = JournalRetentionMonitor.keepNewest(env);
        Path scratch = Files.createTempDirectory(tempRoot, "slg-readonly-secondary-");
        RocksDB.loadLibrary();
        try (Options opts = new Options().setCreateIfMissing(false).setMaxOpenFiles(-1).setDbLogDir(scratch.toString());
             RocksDB source = RocksDB.openAsSecondary(opts, primary.toString(), scratch.toString());
             ReadOptions reads = new ReadOptions().setFillCache(false)) {
            source.tryCatchUpWithPrimary();
            verifyMetadata(source, "meta/realm", Integer.toString(realmId));
            verifyMetadata(source, "meta/volume", volumeId.toString());
            verifyRetentionFormat(source);
            var disk = RealmDiskGuard.observe(primary, env);
            RealmJournal.Inspection page;
            long through = 0;
            do {
                // 完整日志（6/8）从 J1 开始，已清前缀（7/9）从持久的 J(F+1) 开始。
                // 身份跨代不影响只读检查；预演不会给旧卷补写任何元数据。
                page = RealmJournal.inspectPage(source, reads, through, 128);
                through = page.verifiedThrough();
            } while (page.issue() == null && through < page.high());

            String recoveryDiagnostic = null;
            JournalRetentionPreview.RecoveryPoint recovery = null;
            if (checkpoint != null && page.issue() == null) {
                if (Boolean.TRUE.equals(disk.get("clientWritesLimited"))) {
                    recoveryDiagnostic = "Disk pressure or no reliable sample; restore drill skipped";
                } else {
                    try { recovery = JournalRecoveryDrill.verify(source, primary, checkpoint, realmId, volumeId,
                            ((Number) disk.get("warnBytes")).longValue()); }
                    catch (IOException failed) { recoveryDiagnostic = failed.getMessage(); }
                }
            }
            // 恢复演练可能耗时。最后才核对实时消费者；若它已超过固定原卷水位，本轮必须重新取证。
            var consumer = page.issue() == null ? RetentionCursorProbe.observe(env, realmId, volumeId,
                    seq -> RealmJournal.fixedEntry(source, seq), page.high())
                    : new JournalRetentionPreview.Consumer(realmId, volumeId, null, "JOURNAL_INVALID");
            var plan = JournalRetentionPreview.calculate(realmId, volumeId, page, JournalRetentionMonitor.REQUIRED_CONSUMERS,
                    Map.of("mysql", consumer), recovery, keep);
            var report = new LinkedHashMap<String, Object>();
            report.put("mode", "OBSERVE_ONLY");
            report.put("disk", disk);
            report.put("journalRetention", plan);
            if (recoveryDiagnostic != null) report.put("snapshotDiagnostic", recoveryDiagnostic);
            return report;
        } catch (RocksDBException e) {
            throw new IOException("Cannot read the realm through a secondary", e);
        } finally {
            // 仅回收本次自己建的 secondary 工作目录。原卷、快照及它们的 WAL 都不是这个目录。
            try (var files = Files.walk(scratch)) {
                for (Path file : files.sorted(Comparator.reverseOrder()).toList()) Files.delete(file);
            }
        }
    }

    static void verifyMetadata(RocksDB source, String key, String expected) throws RocksDBException, IOException {
        byte[] raw = source.get(key.getBytes(StandardCharsets.US_ASCII));
        if (raw == null || !expected.equals(new String(raw, StandardCharsets.US_ASCII)))
            throw new IOException("Missing or incompatible " + key + "; preview stopped");
    }

    /** 这里只读取已经就绪的正式格式；缺失或未知的格式直接停止，预演不会改写原卷元数据。 */
    static String verifyRetentionFormat(RocksDB source) throws RocksDBException, IOException {
        byte[] raw = source.get("meta/format".getBytes(StandardCharsets.US_ASCII));
        String version = raw == null ? "" : new String(raw, StandardCharsets.US_ASCII);
        if (!"6".equals(version) && !"7".equals(version)
                && !RealmVolume.ROLE_FORMAT.equals(version)
                && !RealmVolume.PRUNED_ROLE_FORMAT.equals(version))
            throw new IOException("Unsupported retention inspection format; preview stopped");
        return version;
    }
}
