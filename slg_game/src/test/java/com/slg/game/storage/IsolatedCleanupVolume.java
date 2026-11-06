package com.slg.game.storage;

import com.slg.game.projection.LockedTestProjectionCursor;
import com.slg.game.projection.ProjectionConflict;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import javax.sql.DataSource;
import org.rocksdb.Checkpoint;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * 单写者清理实验，仅编译到 test-classes。没有“打开已有卷后执行清理”的入口：实验对象必须
 * 亲自在系统临时目录下创建新目录、新身份和新数据库，之后所有写操作只使用该对象持有的原生句柄。
 * 实验期间不运行并发写者，预演和删除之间的原卷视图因此固定。真实 GAME 的启动命令没有此能力。
 */
public final class IsolatedCleanupVolume implements AutoCloseable {
    private final Thread owner = Thread.currentThread();
    private final Path directory, snapshot;
    private final int realmId;
    private final UUID volumeId;
    private final Options options;
    private final RocksDB db;
    private final RealmJournal journal;

    private IsolatedCleanupVolume(Path directory, int realmId, UUID volumeId, Options options, RocksDB db)
            throws IOException, RocksDBException {
        this.directory = directory;
        this.snapshot = directory.resolveSibling("checkpoint");
        this.realmId = realmId;
        this.volumeId = volumeId;
        this.options = options;
        this.db = db;
        this.journal = new RealmJournal(db, (writes, changes) -> {
            requireOwner();
            db.write(writes, changes);
        });
        journal.recover();
    }

    public static IsolatedCleanupVolume create(Path testRoot, int realmId, UUID volumeId)
            throws IOException, RocksDBException {
        Path root = testRoot.toRealPath();
        Path tempRoot = Path.of(System.getProperty("java.io.tmpdir")).toRealPath();
        if (!root.startsWith(tempRoot) || root.equals(tempRoot) || !testRoot.toAbsolutePath().equals(root))
            throw new IOException("Expected a canonical, dedicated test directory under the JVM temporary directory");
        Path directory = Files.createDirectory(root.resolve("realm")); // 已存在即失败，绝不接管已有区服。
        RealmVolume.initialize(directory, realmId, volumeId);
        Options options = new Options().setCreateIfMissing(false).setManualWalFlush(false);
        RocksDB db = null;
        try {
            db = RocksDB.open(options, directory.toString());
            return new IsolatedCleanupVolume(directory, realmId, volumeId, options, db);
        } catch (IOException | RocksDBException | RuntimeException | Error failure) {
            if (db != null) db.close();
            options.close();
            throw failure;
        }
    }

    public RealmJournal journal() { return journal; }

    /** 实际生成与原卷不同目录的 native checkpoint；仅存在文件不能证明 S 可恢复。 */
    public void checkpoint() throws IOException, RocksDBException {
        requireOwner();
        if (Files.exists(snapshot, LinkOption.NOFOLLOW_LINKS)) throw new IOException("Test checkpoint already exists");
        try (Checkpoint cp = Checkpoint.create(db)) { cp.createCheckpoint(snapshot.toString()); }
    }

    /** 用原始业务键独立检查可见性，避免仅凭消费位置或前缀标记宣称已经做了真实删除。 */
    public boolean retains(long sequence) throws RocksDBException {
        return db.get(RealmJournal.journalKey(sequence)) != null;
    }

    private void requireOwner() {
        if (Thread.currentThread() != owner)
            throw new IllegalStateException("Only the isolated experiment's single writer can mutate its test volume");
    }

    /**
     * 只有本次检查的保留区间完整、快照实际可恢复且 SQL 游标已锁定并核对，才允许动 J 键。
     * 首次清理的闭区间是 [1, min(H−1, H−K, c−1, S)]，尤其不能清掉重连必须读取的 Jc。
     * 条件不足返回预演中的阻断原因；读取/恢复失败也可能抛出异常，调用者须停止实验。
     * 一旦开始写批次，异常不能证明删除未发生，必须重开卷核对实际 F 和保留键。
     * 返回值描述执行所依据的预演；测试另读原始键与持久水位确认删除已经生效。
     */
    public JournalRetentionPreview.Result clean(DataSource mysql, long keepNewest) throws IOException, RocksDBException {
        requireOwner();
        RealmJournal.Inspection history;
        long verified = 0;
        do {
            history = journal.inspectPage(verified, 128);
            verified = history.verifiedThrough();
        } while (history.issue() == null && verified < history.high());

        JournalRetentionPreview.RecoveryPoint recovery = null;
        if (history.issue() == null && history.prunedThrough() == 0 && Files.isDirectory(snapshot, LinkOption.NOFOLLOW_LINKS)) {
            // 在拷贝中恢复/重放，并逐键核对 S 和 H。现阶段不能从已清前缀的卷重新证明更新的快照。
            recovery = JournalRecoveryDrill.verify(db, directory, snapshot, realmId, volumeId);
        }
        boolean writeStarted = false;
        try (Connection sql = mysql.getConnection()) {
            sql.setAutoCommit(false);
            var consumer = LockedTestProjectionCursor.read(sql, realmId, volumeId, journal, history);
            var plan = plan(history, consumer, recovery, keepNewest);
            if (plan.lastEligibleSeq() != null) {
                // 不接受调用者提供的删除区间，仅执行刚刚核对的连续前缀。end 是排他边界，
                // 因此保护的 Jc 和 JH 不在区间内。S 状态、R 回执、待办索引和引擎 WAL 不参与删除。
                long last = plan.lastEligibleSeq();
                try (var batch = new WriteBatch(); var writes = new WriteOptions().setDisableWAL(false).setSync(false)) {
                    batch.deleteRange(RealmJournal.journalKey(plan.firstEligibleSeq()), RealmJournal.journalKey(last + 1));
                    batch.put(RealmJournal.PRUNED_THROUGH_KEY.getBytes(StandardCharsets.US_ASCII),
                            ByteBuffer.allocate(Long.BYTES).putLong(last).array());
                    batch.put("meta/format".getBytes(StandardCharsets.US_ASCII), RealmVolume.PRUNED_ROLE_FORMAT.getBytes(StandardCharsets.US_ASCII));
                    // 一批同时写范围墓碑和格式/边界，崩溃不会把“意外缺号”误认成“已清理”。
                    writeStarted = true;
                    db.write(writes, batch);
                }
            }
            sql.rollback(); // SQL 只有读和锁。绝不能因实验清理而人为推进 MySQL 消费位置。
            return plan;
        } catch (SQLException failure) {
            if (writeStarted) throw new IOException("SQL lock release unconfirmed after local cleanup; inspect on reopen", failure);
            String state = failure instanceof ProjectionConflict ? "SQL_BOUNDARY_CONFLICT" : "SQL_UNAVAILABLE";
            return plan(history, new JournalRetentionPreview.Consumer(realmId, volumeId, null, state), recovery, keepNewest);
        }
    }

    private JournalRetentionPreview.Result plan(RealmJournal.Inspection history, JournalRetentionPreview.Consumer mysql,
                                               JournalRetentionPreview.RecoveryPoint recovery, long keepNewest) {
        return JournalRetentionPreview.calculate(realmId, volumeId, history, JournalRetentionMonitor.REQUIRED_CONSUMERS,
                Map.of("mysql", mysql), recovery, keepNewest);
    }

    @Override public void close() {
        journal.detach();
        db.close();
        options.close();
    }
}
