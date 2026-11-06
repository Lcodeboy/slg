package com.slg.game.storage;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.slg.core.protocol.Json;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongPredicate;
import java.util.function.LongFunction;

/** 区服业务日志。各对象可以并行准备决定，连续序号只在短提交阶段分配。 */
public final class RealmJournal {
    private static final byte[] HIGH = "meta/journal-high".getBytes(StandardCharsets.US_ASCII);
    static final String PRUNED_THROUGH_KEY = "meta/journal-pruned-through";
    private static final byte[] PRUNED_THROUGH = PRUNED_THROUGH_KEY.getBytes(StandardCharsets.US_ASCII);
    private static final byte[] FORMAT = "meta/format".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] JOURNAL_PREFIX = {'J'};
    private static final byte[] JOURNAL_END = {'K'};
    private static final byte[] NO_BYTES = {};
    private static final int MAX_PAGE_BYTES = 2 * 1024 * 1024;
    private static final int MAX_JSON_BYTES = 65_536;
    static final int MAX_ENTRY_BYTES = 512 * 1024;
    private static final int MAX_CHANGES = 128;
    private static final int ENTRY_SCHEMA = 1;
    private static final ObjectReader ENTRY_READER = strictReader(Entry.class);
    private static final ObjectReader RECEIPT_READER = strictReader(Receipt.class);

    public record Value(long version, String json) {
        public Value {
            if (version < 1) throw new IllegalArgumentException("Invalid state version");
            checkJson(json);
        }
    }

    /** 日志水位及该水位所见的对象；value 为 null 表示当时尚未创建。 */
    public record SnapshotValue(long sequence, Value value) {}

    public record PendingBattle(String battleId, String playerId, String inputHash, String rulesVersion, Value record) {}

    /** 扫描时发现的任务候选；执行业务前还须从权威状态复核版本、状态和时间。 */
    public record DueTask(String taskId, String playerId, String marchId, String type, int generation, long dueAt) {
        public DueTask {
            identifier(taskId, 251); // state 键还包含 "task:" 前缀
            identifier(playerId, 64);
            identifier(marchId, 128);
            identifier(type, 128);
            if (generation <= 0 || dueAt < 0) throw new IllegalArgumentException("Invalid task generation or time");
        }
    }

    /** expectedVersion=0 表示仅允许首次创建；后版本固定为预期版本加一。 */
    public record Change(String key, long expectedVersion, String json) {
        public Change {
            identifier(key, 256);
            if (expectedVersion < 0 || expectedVersion == Long.MAX_VALUE)
                throw new IllegalArgumentException("Invalid expected version");
            checkJson(json);
        }
    }

    public record Decision(String scope, String requestId, String digest, String factType,
                           String factJson, String resultJson, List<Change> changes) {
        public Decision {
            identifier(scope, 256);
            identifier(requestId, 128);
            identifier(digest, 128);
            identifier(factType, 128);
            checkJson(factJson);
            checkJson(resultJson);
            if (changes == null || changes.size() > MAX_CHANGES) throw new IllegalArgumentException("Invalid change count");
            changes = List.copyOf(changes);
            var keys = new HashSet<String>();
            int payloadBytes = utf8(factJson).length + utf8(resultJson).length;
            for (Change change : changes) {
                if (!keys.add(change.key())) throw new IllegalArgumentException("Duplicate state key");
                payloadBytes += utf8(change.json()).length;
                if (payloadBytes > MAX_ENTRY_BYTES) throw new IllegalArgumentException("Decision exceeds size limit");
            }
        }
    }

    public record Receipt(long sequence, String digest, String resultJson) {
        public Receipt {
            positive(sequence);
            identifier(digest, 128);
            checkJson(resultJson);
        }
    }

    /** applied 为 true 才能把本次草案发布到内存；命中历史回执时绝不能重放旧快照。 */
    public record CommitOutcome(Receipt receipt, boolean applied) {}

    /** 包含已决定的状态变更与业务事实，投影器无需按当前规则重新运行原请求。 */
    public record Entry(int schemaVersion, long sequence, String scope, String requestId, String digest, String factType,
                        String factJson, String resultJson, List<Change> changes) {
        public Entry {
            if (schemaVersion != ENTRY_SCHEMA) throw new IllegalArgumentException("Unsupported journal entry schema");
            positive(sequence);
            changes = List.copyOf(changes);
        }
    }

    public static final class Conflict extends IllegalStateException {
        Conflict() { super("Request ID already used with a different digest"); }
    }

    public static final class Stale extends IllegalStateException {
        Stale(String key) { super("State version changed: " + key); }
    }

    private final RocksDB db;
    private final BatchWriter writer;
    private final ReentrantReadWriteLock access = new ReentrantReadWriteLock();
    // 冷读持有 native 快照直至释放；仅关卷需等待这个锁，追加日志不参与。
    private final ReentrantReadWriteLock snapshotLifetime = new ReentrantReadWriteLock(true);
    private long high;
    private boolean writable, closed;

    @FunctionalInterface
    interface BatchWriter {
        void write(WriteOptions options, WriteBatch batch) throws RocksDBException;
    }

    RealmJournal(RocksDB db) { this(db, db::write); }

    // 生产固定使用 db.write。包内测试可在真实批次已写入、结果返回前模拟 I/O 异常。
    RealmJournal(RocksDB db, BatchWriter writer) {
        this.db = db;
        this.writer = writer;
    }

    static void initializeWatermark(WriteBatch batch) throws RocksDBException {
        batch.put(HIGH, ByteBuffer.allocate(Long.BYTES).putLong(0).array());
    }

    /**
     * 打开期间独占。清理过的卷只能缺少 J1..JF：格式号、持久前缀水位 F、实际首键和末键必须对应。
     * 对已清前缀的格式 7/9 再逐页核对整个保留后缀，确认 J(F+1)..JH 连续且可解码，才让客户端进入。不能通过猜测
     * 第一个物理键来“修复”水位，否则后台任务可能在一次有缺口的历史上再次结算。
     */
    void recover() throws IOException, RocksDBException {
        writable = false;
        try (ReadOptions reads = new ReadOptions().setFillCache(false)) {
            Inspection page = inspectPage(db, reads, 0, 1);
            if (page.issue() != null) throw new InvalidPrefix(page.issue());
            high = page.high();
            if (page.prunedThrough() > 0) {
                while (page.verifiedThrough() < high) {
                    page = inspectPage(db, reads, page.verifiedThrough(), 128);
                    if (page.issue() != null) throw new InvalidPrefix(page.issue());
                }
            } else if (high > 1) {
                // 完整卷在这里做首尾校验。历史中的中间条目仍在消费或显式预演时按页校验。
                fixedEntry(db, reads, high);
            }
        }
        writable = true;
    }

    public long highSequence() throws IOException { return withReadLock(() -> high); }

    /** 从已恢复且验证过的卷读取建角所用的本区编号，服务只在启动时调用一次。 */
    public int roleRealmId() throws IOException {
        return withReadLock(() -> {
            byte[] format = db.get(FORMAT);
            if (format == null) throw new IOException("Missing realm format");
            String version = new String(format, StandardCharsets.US_ASCII);
            if (!RealmVolume.ROLE_FORMAT.equals(version) && !RealmVolume.PRUNED_ROLE_FORMAT.equals(version))
                throw new IOException("Realm cannot serve typed gameplay receipts");
            byte[] raw = db.get(utf8("meta/realm"));
            if (raw == null) throw new IOException("Missing realm identity");
            try {
                int realm = Integer.parseInt(new String(raw, StandardCharsets.US_ASCII));
                if (realm <= 0) throw new NumberFormatException();
                return realm;
            } catch (NumberFormatException invalid) {
                throw new IOException("Invalid stored realm ID", invalid);
            }
        });
    }

    /** 返回不可变快照；尚未创建的状态返回 null。 */
    public Value read(String key) throws IOException {
        identifier(key, 256);
        return withReadLock(() -> decodeValue(db.get(stateKey(key))));
    }

    /** 原子取得日志水位与 RocksDB 快照，随后在区服提交锁外加载对象。 */
    public SnapshotValue readSnapshot(String key) throws IOException {
        return readSnapshot(key, RocksDB::get);
    }

    @FunctionalInterface
    interface SnapshotGet {
        byte[] get(RocksDB db, ReadOptions options, byte[] key) throws IOException, RocksDBException;
    }

    SnapshotValue readSnapshot(String key, SnapshotGet get) throws IOException {
        identifier(key, 256);
        return withSnapshot((options, sequence) ->
                new SnapshotValue(sequence, decodeValue(get.get(db, options, stateKey(key)))));
    }

    public record LinkedSnapshot(long sequence, Value binding, String targetKey, Value target) {}

    @FunctionalInterface
    public interface TargetKey { String resolve(Value binding) throws IOException; }

    /**
     * 在同一原生快照中先读目录，再读它指向的权威对象。目录缺失时由调用方指定本区新建候选键：
     * 若只有目录或只有角色，加载方可以报告损坏，而不会把半份记录误判为可建角。
     * 两次读取都在加载池，不占区服提交锁。
     */
    public LinkedSnapshot readLinkedSnapshot(String bindingKey, TargetKey targetKey) throws IOException {
        identifier(bindingKey, 256);
        return withSnapshot((options, sequence) -> {
            Value binding = decodeValue(db.get(options, stateKey(bindingKey)));
            String key = targetKey.resolve(binding);
            identifier(key, 256);
            Value target = decodeValue(db.get(options, stateKey(key)));
            return new LinkedSnapshot(sequence, binding, key, target);
        });
    }

    /** 只扫描 (dueAt, taskId) 的有序页，不占用提交锁做磁盘 I/O。after 是上一页末项，排他。 */
    public List<DueTask> dueTasks(long now, DueTask after, int limit) throws IOException {
        if (now < 0 || limit <= 0 || limit > 1024) throw new IllegalArgumentException("Invalid due task scan bounds");
        return withSnapshot((options, sequence) -> RealmTaskIndex.due(db, options, now, after, limit));
    }

    /** 一个快照中的待计算任务页；游标是最后检查过的 battleId，完成的战斗不占扫描预算。 */
    public List<PendingBattle> pendingBattles(String after, int limit) throws IOException {
        if (after != null) identifier(after, 128);
        if (limit < 1 || limit > 1024) throw new IllegalArgumentException("Invalid battle scan limit");
        return withSnapshot((options, sequence) -> RealmBattleIndex.page(db, options, after, limit));
    }

    @FunctionalInterface
    private interface SnapshotRead<T> { T read(ReadOptions options, long seq) throws IOException, RocksDBException; }

    private <T> T withSnapshot(SnapshotRead<T> read) throws IOException {
        var lifetime = snapshotLifetime.readLock();
        lifetime.lock();
        try {
            Snapshot snapshot;
            long sequence;
            var lock = access.readLock();
            lock.lock();
            try {
                ensureAvailable();
                sequence = high;
                snapshot = db.getSnapshot();
                if (snapshot == null) throw new IOException("Cannot acquire realm snapshot");
            } finally {
                lock.unlock();
            }
            try (ReadOptions options = new ReadOptions()) {
                options.setSnapshot(snapshot);
                T result = read.read(options, sequence);
                // 慢读取期间可能有写入未获确认；即使这份旧快照完整，也不能交给业务执行。
                return withReadLock(() -> result);
            } finally {
                db.releaseSnapshot(snapshot);
            }
        } catch (RocksDBException e) {
            throw new IOException("Cannot read realm snapshot", e);
        } finally {
            lifetime.unlock();
        }
    }

    /** 同一个 scope 内的请求 ID 只可对应一个摘要和结果。 */
    public Receipt lookup(String scope, String requestId, String digest) throws IOException {
        identifier(digest, 128);
        byte[] key = receiptKey(scope, requestId);
        return withReadLock(() -> matchingReceipt(key, digest));
    }

    /**
     * 只读查询所属作用域的原回执。R 键和当时的业务决定同批提交，即使对应的 J 条目已经清理也可单键取回。
     * 取快照时同时捕获已提交水位，慢读取不阻塞新区服提交。返回 null 仅表示此快照中没有回执，
     * 可能还有在途请求；卷不可用、记录损坏或序号超出水位时抛出异常，不能对客户端报成查无记录。
     */
    public Receipt findReceipt(String scope, String requestId) throws IOException {
        byte[] key = receiptKey(scope, requestId);
        return withSnapshot((options, capturedHigh) -> decodeReceipt(db.get(options, key), capturedHigh));
    }

    /** 只返回已提交记录；消费者遇到水位以内的空洞必须停在原位置。 */
    public Entry entry(long sequence) throws IOException {
        positive(sequence);
        // 重连要精确读取 Jc。不能通过“从 c−1 翻一页”实现，否则会引入对前一条锚点的额外依赖。
        return withSnapshot((options, capturedHigh) -> {
            if (sequence > capturedHigh) return null;
            if (sequence <= readPrunedThrough(db, options, capturedHigh))
                throw new InvalidPrefix("Requested journal sequence was pruned: " + sequence);
            return fixedEntry(db, options, sequence);
        });
    }

    /** 消费者不能跳过坏日志或缺口去投影更新的状态，否则 MySQL 会出现从未在本区成立过的数据。 */
    public static final class InvalidPrefix extends IOException {
        InvalidPrefix(String reason) { super(reason); }
    }

    /**
     * 从排他游标读取一个连续前缀。迭代器只在固定的 RocksDB 快照内使用，读历史记录和解码
     * 都在区服提交锁外完成。限制条数和编码字节数，积压再多也不会创建一个同样大的 JVM 队列。
     * 返回前已释放 native 快照，后续慢 SQL 事务不会占着区服锁或妨碍 RocksDB 回收旧版本。
     */
    public List<Entry> entriesAfter(long after, int limit) throws IOException {
        checkPageBounds(after, limit);
        return withSnapshot((options, highAtRead) -> {
            if (after > highAtRead) throw new InvalidPrefix("Projection checkpoint is ahead of the realm journal");
            long floor = readPrunedThrough(db, options, highAtRead);
            // SQL 恢复到了低于已清前缀的备份时，必须显式报告缺口，不能私自把消费者推进到 F。
            if (after < floor) throw new InvalidPrefix("Requested cursor is behind pruned journal prefix " + floor);
            if (after == highAtRead) return List.of();
            var page = new ArrayList<Entry>(limit);
            try (RocksIterator it = db.newIterator(options.setFillCache(false))) {
                Prefix prefix = readPrefix(it, after, floor, highAtRead, limit, page);
                if (prefix.issue() != null) throw new InvalidPrefix(prefix.issue());
            }
            return List.copyOf(page);
        });
    }

    /**
     * high 是本页快照中的提交水位。first、last 是实际存在的业务日志首尾序号；空日志范围用 null。
     * prunedThrough 是已原子清理的闭区间上界 F，旧卷为 0；verifiedThrough 自 F 起计数。
     * F 表示持久登记的清理边界，不声称已清理的条目仍可验证。
     * 只有 issue 为空且 verifiedThrough 等于 high 时，这个水位的保留日志才全部检查完毕。
     */
    public record Inspection(long high, Long first, Long last, long verifiedThrough, String issue, long prunedThrough) {
        /** 未经过清理的完整卷沿用起点 0。 */
        public Inspection(long high, Long first, Long last, long verifiedThrough, String issue) {
            this(high, first, last, verifiedThrough, issue, 0);
        }
    }

    /**
     * 每次只核对一页，读取磁盘和解码都在区服提交锁外。调用者保存同一卷本次打开期间的检查进度，
     * 下次传回 verifiedThrough。正常 GAME 运行期间 F 固定，新增记录可以接在已检查的后缀后面。
     * 重开、换卷或隔离实验改变 F 后从 0 重新检查。坏页和 I/O 错误要停止预演，不能跳过。
     */
    public Inspection inspectPage(long verifiedThrough, int limit) throws IOException {
        checkPageBounds(verifiedThrough, limit);
        return withSnapshot((options, capturedHigh) -> {
            var result = inspectPage(db, options, verifiedThrough, limit);
            if (result.high() != capturedHigh) throw new IOException("Snapshot watermark differs from realm journal");
            return result;
        });
    }

    /**
     * 供只读观察器复用：调用方必须固定同一快照（或在整页读取期间不追赶的 RocksDB secondary）。
     * 水位、首尾键和被核对的值都从同一视图读取，不把两个时刻的结果拼成“连续”。
     * 本方法只查 'J' 键空间，不打开写句柄，不访问引擎 WAL 文件。
     */
    public static Inspection inspectPage(RocksDB source, ReadOptions options, long verifiedThrough, int limit)
            throws IOException, RocksDBException {
        checkPageBounds(verifiedThrough, limit);
        byte[] watermark = source.get(options, HIGH);
        if (watermark == null || watermark.length != Long.BYTES)
            throw new IOException("Missing or malformed journal watermark");
        long high = ByteBuffer.wrap(watermark).getLong();
        if (high < 0) throw new IOException("Negative journal watermark");
        long floor = readPrunedThrough(source, options, high);

        try (var scan = new ReadOptions(options).setFillCache(false); RocksIterator it = source.newIterator(scan)) {
            it.seek(JOURNAL_PREFIX);
            byte[] firstKey = keyAt(it);
            it.seek(JOURNAL_END);
            if (it.isValid()) it.prev();
            else { it.status(); it.seekToLast(); }
            byte[] lastKey = keyAt(it);
            Long first = boundarySequence(firstKey), last = boundarySequence(lastKey);
            if ((firstKey != null && first == null) || (lastKey != null && last == null))
                return new Inspection(high, first, last, floor, "Malformed journal boundary key", floor);
            if (high == 0 ? first != null || last != null : !Long.valueOf(floor + 1).equals(first))
                return new Inspection(high, first, last, floor, "Journal start disagrees with retained prefix", floor);
            if (high > 0 && !Long.valueOf(high).equals(last))
                return new Inspection(high, first, last, floor, "Journal tail disagrees with watermark", floor);
            if (verifiedThrough > high)
                return new Inspection(high, first, last, floor, "Journal watermark moved behind verified prefix", floor);
            if (verifiedThrough == high) return new Inspection(high, first, last, high, null, floor);
            // 观察器可以从 0 发起新检查：只在持久边界与物理范围一致后，才从 F+1 核对保留后缀。
            Prefix page = readPrefix(it, Math.max(verifiedThrough, floor), floor, high, limit, null);
            return new Inspection(high, first, last, page.through(), page.issue(), floor);
        }
    }

    /** 只有已清前缀的格式 7/9 承认明确的 F。格式与边界由首次清理同批写入，单独一项不能解释为合法清理。 */
    static long readPrunedThrough(RocksDB source, ReadOptions options, long high) throws RocksDBException, IOException {
        byte[] format = source.get(options, FORMAT);
        byte[] raw = source.get(options, PRUNED_THROUGH);
        if (format == null) throw new InvalidPrefix("Missing realm format");
        String version = new String(format, StandardCharsets.US_ASCII);
        if (!version.equals("7") && !version.equals(RealmVolume.PRUNED_ROLE_FORMAT)) {
            if (!version.equals("6") && !version.equals(RealmVolume.ROLE_FORMAT))
                throw new InvalidPrefix("Unsupported journal format");
            if (raw != null) throw new InvalidPrefix("Pruned prefix on an unpruned journal format");
            return 0;
        }
        if (raw == null || raw.length != Long.BYTES) throw new InvalidPrefix("Missing or malformed pruned prefix");
        long floor = ByteBuffer.wrap(raw).getLong();
        if (floor <= 0 || floor >= high) throw new InvalidPrefix("Pruned prefix would remove the journal tail");
        return floor;
    }

    private static void checkPageBounds(long after, int limit) {
        if (after < 0 || limit < 1 || limit > 128) throw new IllegalArgumentException("Invalid journal page bounds");
    }

    private record Prefix(long through, String issue) {}

    /** 投影和预演使用同一套逐条校验。预演不收集条目，避免在后台长期持有已解码的历史。 */
    private static Prefix readPrefix(RocksIterator it, long after, long floor, long high, int limit, List<Entry> entries)
            throws RocksDBException {
        if (after == floor) {
            // 第一个保留键前没有锚点：JF 已随前缀清除。必须从 J 键空间开头起步，以便发现残留或漏删的键。
            it.seek(JOURNAL_PREFIX);
        } else {
            // 从上一页末键 next，而不是直接 seek 下一序号。否则夹在两页之间的畸形键会被跳过。
            byte[] anchor = journalKey(after);
            it.seek(anchor);
            if (!Arrays.equals(keyAt(it), anchor)) return new Prefix(floor, "Missing journal page anchor " + after);
            it.next();
        }
        long through = after;
        int bytes = 0;
        while (through < high && through - after < limit) {
            long expected = through + 1;
            if (!Arrays.equals(journalKey(expected), keyAt(it)))
                return new Prefix(through, "Missing or malformed realm journal key at sequence " + expected);
            // JNI 返回原始长度，即使目标缓冲区为空。先检查长度，避免巨大坏值耗尽 JVM 堆。
            int size = it.value(NO_BYTES);
            if (size <= 0 || size > MAX_ENTRY_BYTES) return new Prefix(through, "Invalid journal size at sequence " + expected);
            if (bytes + size > MAX_PAGE_BYTES) break;
            try {
                Entry entry = decodeEntry(it.value(), expected);
                if (entries != null) entries.add(entry);
            } catch (IOException invalid) {
                return new Prefix(through, "Invalid realm journal entry at sequence " + expected);
            }
            bytes += size;
            through = expected;
            it.next();
        }
        it.status();
        return new Prefix(through, null);
    }

    /** 最长只复制 10 字节，就能区别标准的 9 字节键、过长键和非本键空间的记录。 */
    private static byte[] keyAt(RocksIterator it) throws RocksDBException {
        if (!it.isValid()) { it.status(); return null; }
        byte[] buffer = new byte[10];
        int size = it.key(buffer);
        return size > 0 && buffer[0] == 'J' ? Arrays.copyOf(buffer, Math.min(size, buffer.length)) : null;
    }

    private static Long boundarySequence(byte[] key) {
        if (key == null || key.length != 9) return null;
        long seq = ByteBuffer.wrap(key, 1, Long.BYTES).getLong();
        return seq > 0 ? seq : null;
    }

    public Receipt commit(Decision decision) throws IOException {
        return commitWithOutcome(decision).receipt();
    }

    public CommitOutcome commitWithOutcome(Decision decision) throws IOException {
        return commitAdmitted(decision, ignored -> Admission.ALLOW);
    }

    public enum Admission { ALLOW, DISK_PRESSURE, PROJECTION_BACKLOG }

    /** 暂缓不是已提交的业务结果，调用方可以在水位恢复后用原 ID 重试。 */
    public static final class WriteDeferred extends IllegalStateException {
        public final Admission reason;
        private WriteDeferred(Admission reason) { this.reason = reason; }
    }

    /**
     * 在分配新序号的同一把锁内检查预算，避免同时到达的玩家都看到最后一个空位。
     * 谓词仅可读取本地内存水位，不能查询 MySQL。先查回执确保重发已确认操作总能得到原结果。
     */
    public CommitOutcome commitWithOutcome(Decision decision, LongPredicate admitNewWrite) throws IOException {
        return commitAdmitted(decision, high -> admitNewWrite.test(high) ? Admission.ALLOW : Admission.PROJECTION_BACKLOG);
    }

    /** 两类准入都在回执判断之后、任何新序号或写批次之前执行。回调仅使用后台采样出的内存数据。 */
    public CommitOutcome commitAdmitted(Decision decision, LongFunction<Admission> admission) throws IOException {
        Objects.requireNonNull(decision, "decision");
        var lock = access.writeLock();
        lock.lock();
        try {
            ensureAvailable();
            byte[] receiptKey = receiptKey(decision.scope(), decision.requestId());
            Receipt original = matchingReceipt(receiptKey, decision.digest());
            if (original != null) return new CommitOutcome(original, false);
            Admission permission = Objects.requireNonNull(admission.apply(high));
            if (permission != Admission.ALLOW) throw new WriteDeferred(permission);

            if (high == Long.MAX_VALUE) throw new IOException("Journal sequence exhausted");
            long next = high + 1;
            Receipt result = new Receipt(next, decision.digest(), decision.resultJson());
            Entry entry = new Entry(ENTRY_SCHEMA, next, decision.scope(), decision.requestId(), decision.digest(),
                    decision.factType(), decision.factJson(), decision.resultJson(), decision.changes());
            byte[] log = encode(entry);
            if (log.length > MAX_ENTRY_BYTES) throw new IllegalArgumentException("Encoded journal entry exceeds size limit");
            try (WriteBatch batch = new WriteBatch();
                 WriteOptions writes = new WriteOptions().setDisableWAL(false).setSync(false)) {
                for (Change change : decision.changes()) {
                    byte[] key = stateKey(change.key());
                    byte[] current = db.get(key);
                    long version = current == null ? 0 : stateVersion(current);
                    if (version != change.expectedVersion()) throw new Stale(change.key());
                    if (change.key().startsWith("task:")) {
                        RealmTaskIndex.update(batch, change.key(), current, change.json());
                    } else if (change.key().startsWith("battle:")) {
                        RealmBattleIndex.update(batch, change.key().substring("battle:".length()), change.json());
                    }
                    byte[] json = utf8(change.json());
                    batch.put(key, ByteBuffer.allocate(Long.BYTES + json.length).putLong(version + 1).put(json).array());
                }
                batch.put(journalKey(next), log);
                batch.put(receiptKey, encode(result));
                batch.put(HIGH, ByteBuffer.allocate(Long.BYTES).putLong(next).array());
                // 写调用一旦开始，任何没有明确返回的结果都要求重开原卷，不能复用 next。
                writable = false;
                writer.write(writes, batch);
                high = next;
                writable = true;
            }
            return new CommitOutcome(result, true);
        } catch (RocksDBException e) {
            throw new IOException("Realm journal I/O failed", e);
        } finally {
            lock.unlock();
        }
    }

    /** 写重试必须与最初输入匹配；调用方在锁内读取回执和对应的已提交水位。 */
    private Receipt matchingReceipt(byte[] key, String digest) throws RocksDBException, IOException {
        Receipt result = decodeReceipt(db.get(key), high);
        if (result != null && !result.digest().equals(digest)) throw new Conflict();
        return result;
    }

    private static Receipt decodeReceipt(byte[] raw, long committedHigh) throws IOException {
        if (raw == null) return null;
        if (raw.length > MAX_ENTRY_BYTES) throw new IOException("Oversized journal receipt");
        Receipt result = RECEIPT_READER.readValue(raw);
        if (result == null || result.sequence() > committedHigh) throw new IOException("Receipt outside committed journal");
        return result;
    }

    /** 调用方持有一次固定的 secondary 视图；按与线上提交相同的编码约束读取锚点或重放事实。 */
    static Entry fixedEntry(RocksDB source, long sequence) throws IOException {
        positive(sequence);
        try (var reads = new ReadOptions().setFillCache(false)) { return fixedEntry(source, reads, sequence); }
        catch (RocksDBException e) { throw new IOException("Cannot read journal sequence " + sequence, e); }
    }

    /** 单读 Jc 不依赖相邻业务键。先核对键和长度，再复制到 Java 堆，损坏的大记录也要有界失败。 */
    private static Entry fixedEntry(RocksDB source, ReadOptions reads, long sequence) throws IOException, RocksDBException {
        byte[] key = journalKey(sequence);
        try (var it = source.newIterator(reads)) {
            it.seek(key);
            if (!Arrays.equals(keyAt(it), key)) throw new InvalidPrefix("Missing journal entry " + sequence);
            int size = it.value(NO_BYTES);
            if (size <= 0 || size > MAX_ENTRY_BYTES) throw new InvalidPrefix("Invalid journal size at sequence " + sequence);
            try { return decodeEntry(it.value(), sequence); }
            catch (IOException invalid) { throw new InvalidPrefix("Invalid journal entry " + sequence); }
        }
    }

    private static Entry decodeEntry(byte[] raw, long sequence) throws IOException {
        if (raw == null || raw.length > MAX_ENTRY_BYTES) throw new IOException("Missing or malformed journal entry " + sequence);
        Entry entry = ENTRY_READER.readValue(raw);
        if (entry == null || entry.sequence() != sequence) throw new IOException("Journal entry sequence mismatch");
        try {
            // 外层可解码并不说明这条日志可重放。沿用提交时的标识、事实、结果、状态键唯一性与体积约束。
            new Decision(entry.scope(), entry.requestId(), entry.digest(), entry.factType(),
                    entry.factJson(), entry.resultJson(), entry.changes());
        } catch (IllegalArgumentException e) {
            throw new IOException("Malformed journal decision at sequence " + sequence, e);
        }
        return entry;
    }

    private static long stateVersion(byte[] raw) throws IOException {
        if (raw.length <= Long.BYTES || raw.length > Long.BYTES + MAX_JSON_BYTES) throw new IOException("Malformed state record");
        long version = ByteBuffer.wrap(raw).getLong();
        if (version <= 0) throw new IOException("Malformed state version");
        return version;
    }

    static Value decodeValue(byte[] raw) throws IOException {
        if (raw == null) return null;
        long version = stateVersion(raw);
        try {
            String json = StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(raw, Long.BYTES, raw.length - Long.BYTES)).toString();
            return new Value(version, json);
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid stored state", e);
        }
    }

    private static void positive(long sequence) {
        if (sequence <= 0) throw new IllegalArgumentException("Expected a positive journal sequence");
    }

    private static void identifier(String value, int maxLength) {
        if (value == null || value.isEmpty() || value.length() > maxLength)
            throw new IllegalArgumentException("Invalid journal identifier length");
        for (int i = 0; i < value.length(); i++)
            if (value.charAt(i) < 33 || value.charAt(i) > 126)
                throw new IllegalArgumentException("Expected printable ASCII journal identifier");
    }

    private static void checkJson(String value) {
        if (value == null || value.length() > MAX_JSON_BYTES || value.isBlank() || utf8(value).length > MAX_JSON_BYTES)
            throw new IllegalArgumentException("Invalid journal JSON size");
        // 状态按 UTF-8 编码，拒绝不成对的 UTF-16 代理项，避免保存时静默替换成问号。
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (!Character.isSurrogate(ch)) continue;
            if (!Character.isHighSurrogate(ch) || i + 1 == value.length() || !Character.isLowSurrogate(value.charAt(i + 1)))
                throw new IllegalArgumentException("Invalid Unicode in journal JSON");
            i++;
        }
        Json.read(value, JsonNode.class);
    }

    private static ObjectReader strictReader(Class<?> type) {
        return Json.MAPPER.readerFor(type).with(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES,
                DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES, DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES);
    }

    private static byte[] encode(Object value) throws IOException { return Json.MAPPER.writeValueAsBytes(value); }
    private static byte[] utf8(String text) { return text.getBytes(StandardCharsets.UTF_8); }
    private static byte[] stateKey(String key) { return utf8("S" + key); }
    static byte[] journalKey(long seq) { return ByteBuffer.allocate(9).put((byte)'J').putLong(seq).array(); }

    // scope 使用长度前缀，scope 或 requestId 中出现同一个分隔符也不会碰撞。
    private static byte[] receiptKey(String scope, String requestId) {
        identifier(scope, 256);
        identifier(requestId, 128);
        byte[] owner = utf8(scope), id = utf8(requestId);
        return ByteBuffer.allocate(1 + Integer.BYTES + owner.length + id.length)
                .put((byte)'R').putInt(owner.length).put(owner).put(id).array();
    }

    @FunctionalInterface
    private interface Read<T> { T run() throws IOException, RocksDBException; }

    private <T> T withReadLock(Read<T> read) throws IOException {
        var lock = access.readLock();
        lock.lock();
        try {
            ensureAvailable();
            return read.run();
        } catch (RocksDBException e) {
            throw new IOException("Cannot read realm journal", e);
        } finally {
            lock.unlock();
        }
    }

    private void ensureAvailable() throws IOException {
        if (!writable || closed) throw new IOException("Realm journal is closed or requires recovery");
    }

    /** 等待所有读写结束再释放 native 句柄；已经交出的 journal 引用也随卷失效。 */
    boolean detach() {
        var lifetime = snapshotLifetime.writeLock();
        var lock = access.writeLock();
        lifetime.lock();
        lock.lock();
        try {
            if (closed) return false;
            closed = true;
            return true;
        } finally {
            lock.unlock();
            lifetime.unlock();
        }
    }
}
