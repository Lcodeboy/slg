package com.slg.game.storage;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/**
 * 一个区服的本地 RocksDB 写句柄。初始化是显式的一次性操作；
 * GAME 正常启动只能打开已有原卷，身份检查通过前不能接收业务流量。
 */
public final class RealmVolume implements AutoCloseable {
    private static final String INIT_MARKER = ".realm-volume-init";
    // 8/9 对应采用强类型写回执的完整/已清前缀卷。新旧 GAME 互相拒绝挂载对方的原卷。
    static final String ROLE_FORMAT = "8", PRUNED_ROLE_FORMAT = "9";
    private final Options options;
    private final RocksDB database;
    private final RealmJournal journal;

    static {
        RocksDB.loadLibrary();
    }

    private RealmVolume(Options options, RocksDB database) {
        this.options = options;
        this.database = database;
        this.journal = new RealmJournal(database);
    }

    /**
     * 仅在已存在的专用空目录创建新区服。标志文件的原子创建使同时初始化的进程只有一个能继续。
     * 初始化中断时保留现场；重启不能在半初始化的目录中生成另一份身份。
     */
    public static void initialize(Path directory, int realmId, UUID volumeId) throws IOException {
        initialize(directory, realmId, volumeId, ROLE_FORMAT);
    }

    /** 恢复预演在私有临时目录从零重放旧事实，必须使用源卷的元数据才能逐键比较。 */
    static void initialize(Path directory, int realmId, UUID volumeId, String format) throws IOException {
        if (!ROLE_FORMAT.equals(format) && !"6".equals(format))
            throw new IOException("Expected an unpruned journal format for initialization");
        Path path = validate(directory, realmId, volumeId);
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(path)) {
            if (entries.iterator().hasNext()) {
                throw new IOException("Realm initialization requires an empty directory: " + path);
            }
        }
        Files.createFile(path.resolve(INIT_MARKER));
        try (RealmVolume volume = openDatabase(path, true);
             WriteBatch batch = new WriteBatch();
             WriteOptions writeOptions = new WriteOptions().setDisableWAL(false).setSync(false)) {
            batch.put(bytes("meta/format"), bytes(format));
            batch.put(bytes("meta/realm"), bytes(Integer.toString(realmId)));
            batch.put(bytes("meta/volume"), bytes(volumeId.toString()));
            RealmJournal.initializeWatermark(batch);
            volume.database.write(writeOptions, batch);
        } catch (RocksDBException e) {
            throw new IOException("Cannot initialize realm identity at " + path, e);
        }
    }

    /** 打开指定区服和卷；目录缺失、身份/格式不符、原生写锁冲突时关闭并报错。 */
    public static RealmVolume open(Path directory, int realmId, UUID volumeId) throws IOException {
        return open(directory, realmId, volumeId, false);
    }

    /** 只有恢复预演可重放旧回执格式。调用方创建无链接的私有临时目录，并在结束时移除该副本。 */
    static RealmVolume openRecoveryCopy(Path directory, int realmId, UUID volumeId) throws IOException {
        return open(directory, realmId, volumeId, true);
    }

    private static RealmVolume open(Path directory, int realmId, UUID volumeId, boolean recoveryCopy) throws IOException {
        Path path = validate(directory, realmId, volumeId);
        if (!Files.isRegularFile(path.resolve(INIT_MARKER), LinkOption.NOFOLLOW_LINKS)
                || !Files.isRegularFile(path.resolve("CURRENT"), LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Realm volume is missing or incompletely initialized: " + path);
        }

        RealmVolume volume = openDatabase(path, false);
        boolean verified = false;
        try {
            volume.requireValue("meta/realm", Integer.toString(realmId));
            volume.requireValue("meta/volume", volumeId.toString());
            byte[] format = volume.database.get(bytes("meta/format"));
            verifyFormat(format, recoveryCopy);
            volume.journal.recover();
            verified = true;
            return volume;
        } catch (RocksDBException e) {
            throw new IOException("Cannot read realm volume metadata at " + path, e);
        } finally {
            if (!verified) {
                volume.close();
            }
        }
    }

    /** 返回同一卷共享的提交入口，不能绕开它单独分配日志序号。 */
    public RealmJournal journal() {
        return journal;
    }

    /** 正式写者只接受 8/9。6/7 仅供只读观测器在私有临时副本重放证据，永远不升级元数据。 */
    private static void verifyFormat(byte[] format, boolean recoveryCopy) throws IOException {
        if (Arrays.equals(format, bytes(ROLE_FORMAT)) || Arrays.equals(format, bytes(PRUNED_ROLE_FORMAT))) return;
        if (recoveryCopy && (Arrays.equals(format, bytes("6")) || Arrays.equals(format, bytes("7")))) return;
        throw new IOException("Unsupported realm format for this purpose");
    }

    private static Path validate(Path directory, int realmId, UUID volumeId) throws IOException {
        Objects.requireNonNull(directory, "directory");
        Objects.requireNonNull(volumeId, "volumeId");
        if (realmId <= 0) {
            throw new IllegalArgumentException("realmId must be positive");
        }
        Path path = directory.toAbsolutePath();
        if (!Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Expected an existing realm volume directory: " + path);
        }
        return path;
    }

    private static RealmVolume openDatabase(Path path, boolean initialize) throws IOException {
        Options options = new Options();
        boolean opened = false;
        try {
            options.setCreateIfMissing(initialize).setErrorIfExists(initialize).setManualWalFlush(false);
            RocksDB database = RocksDB.open(options, path.toString());
            opened = true;
            return new RealmVolume(options, database);
        } catch (RocksDBException e) {
            throw new IOException("Cannot open exclusive realm volume at " + path + ": " + e.getMessage(), e);
        } finally {
            if (!opened) {
                options.close();
            }
        }
    }

    private void requireValue(String key, String expected) throws IOException, RocksDBException {
        if (!Arrays.equals(database.get(bytes(key)), bytes(expected))) {
            throw new IOException("Missing or incompatible realm volume metadata: " + key);
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public synchronized void close() {
        if (!journal.detach()) return;
        try {
            database.close();
        } finally {
            options.close();
        }
    }
}
