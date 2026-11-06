package com.slg.game.projection;

import com.slg.core.protocol.Json;
import com.slg.game.storage.RealmJournal.Change;
import com.slg.game.storage.RealmJournal.Entry;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLDataException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import javax.sql.DataSource;

/**
 * 将区服原卷的已提交历史投影为 MySQL 查询数据。一个实例只绑定一个区号、一个原卷；
 * 账号事务和玩家命令不经过此类。每次操作从独立投影连接池借连接，完成后立刻归还。
 *
 * <p>连续进度由数据库确认。断线可能发生在服务器已经提交之后，因此内存里的“上次发送到哪”
 * 不能用作恢复点。重连后重新读游标；如果旧批次再次送来，核验已归档的前缀，只应用新后缀。</p>
 */
public final class MySqlProjectionStore {
    // 原卷的 resource 对象保留自己的版本。占矿/释放随对应行军决定进入同一 SQL 事务；
    // 若跳过它只投影行军，查询侧会出现行军正在采集、矿点却仍为空闲的矛盾状态。
    private static final Set<String> OBJECT_TYPES = Set.of("player", "march", "task", "battle", "resource", "account-role");
    private static final String CHECKPOINT = """
            SELECT c.volume_id, c.last_applied_seq,
                   EXISTS(SELECT 1 FROM projection_event AS e
                          WHERE e.realm_id=c.realm_id AND e.journal_seq=c.last_applied_seq) AS has_tail
            FROM projection_checkpoint AS c WHERE c.realm_id=?
            """;
    private final DataSource dataSource;
    private final int realmId;
    private final String volumeId;

    public MySqlProjectionStore(DataSource dataSource, int realmId, UUID volumeId) {
        this.dataSource = Objects.requireNonNull(dataSource);
        if (realmId < 1) throw new IllegalArgumentException("Invalid projection realm ID");
        this.realmId = realmId;
        this.volumeId = Objects.requireNonNull(volumeId).toString();
    }

    /** 每次消费线程首次连接或故障重连时先核对 DBA 发布的版本。旧库也能写通用对象，却会
     * 把角色 ID 投成账号，因此必须在登记新卷或推进第一个游标前拒绝错误目标库。 */
    void requireRoleIdentitySchema() throws SQLException {
        try (var connection = dataSource.getConnection(); var query = connection.createStatement();
             var rows = query.executeQuery("SELECT version FROM projection_schema_migration WHERE version=3")) {
            if (!rows.next()) throw new ProjectionConflict("Projection schema 003 is not ready; check the identity baseline");
        } catch (SQLException error) {
            // 缺表或无读取版本的授权属于部署错误；网络故障仍交给消费者退避重连。
            if ("42S02".equals(error.getSQLState()) || error.getErrorCode() == 1142)
                throw new ProjectionConflict("Projection schema 003 or its runtime SELECT grant is missing");
            throw error;
        }
    }

    /** 读取 MySQL 已确认的连续前缀。首次接入才登记原卷；空闲轮询只执行一次查询。 */
    public long checkpoint() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            Long cursor = readCheckpoint(connection, false, realmId, volumeId);
            if (cursor != null) return cursor;
            return transact(connection, List.of());
        }
    }

    /**
     * 只接受连续且升序的一批日志。返回提交后数据库的真实水位，可能高于整个批次（迟到的重复投递）。
     * 最后一个对象写入后、游标更新前发生故障，整批一起回滚；commit 应答丢失则交给下次重读判定。
     */
    public long apply(List<Entry> entries) throws SQLException {
        List<Entry> batch = List.copyOf(entries);
        for (int i = 1; i < batch.size(); i++) {
            if (batch.get(i).sequence() - 1 != batch.get(i - 1).sequence())
                throw new ProjectionConflict("Non-contiguous projection batch at sequence " + batch.get(i).sequence());
        }
        try (Connection connection = dataSource.getConnection()) {
            return transact(connection, batch);
        }
    }

    private long transact(Connection connection, List<Entry> batch) throws SQLException {
        connection.setAutoCommit(false);
        try {
            long committed = lockCheckpoint(connection);
            int firstNew = 0;
            while (firstNew < batch.size() && batch.get(firstNew).sequence() <= committed) {
                verifyReplay(connection, realmId, batch.get(firstNew++));
            }
            if (firstNew < batch.size()) {
                if (batch.get(firstNew).sequence() - 1 != committed)
                    throw new ProjectionConflict("Missing journal sequence after " + committed);
                List<Entry> suffix = batch.subList(firstNew, batch.size());
                writeSuffix(connection, suffix);
                committed = suffix.getLast().sequence();
                try (PreparedStatement advance = connection.prepareStatement(
                        "UPDATE projection_checkpoint SET last_applied_seq=? WHERE realm_id=?")) {
                    advance.setLong(1, committed);
                    advance.setInt(2, realmId);
                    if (advance.executeUpdate() != 1) throw new ProjectionConflict("Missing locked checkpoint");
                }
            }
            connection.commit();
            return committed;
        } catch (SQLException | RuntimeException failure) {
            // 回滚也可能因连接已断而失败；保留原始故障。绝不在 finally 中切回 autoCommit，
            // 那会使尚未回滚的半批写入被 JDBC 隐式提交。
            try { connection.rollback(); }
            catch (SQLException rollbackFailure) { failure.addSuppressed(rollbackFailure); }
            if (failure instanceof SQLException sql && invalidData(sql)) {
                String range = batch.isEmpty() ? "checkpoint initialization" : "batch "
                        + batch.getFirst().sequence() + ".." + batch.getLast().sequence();
                var conflict = new ProjectionConflict("Projection data or integrity error for realm " + realmId + ", " + range);
                conflict.initCause(sql);
                throw conflict;
            }
            throw failure;
        }
    }

    /** 用 InnoDB 行锁串行同区的两个消费者。重复初始化只能命中原行，绝不改卷 ID 或把进度归零。 */
    private long lockCheckpoint(Connection connection) throws SQLException {
        try (PreparedStatement init = connection.prepareStatement("""
                INSERT INTO projection_checkpoint (realm_id, volume_id, last_applied_seq) VALUES (?, ?, 0)
                ON DUPLICATE KEY UPDATE realm_id=projection_checkpoint.realm_id
                """)) {
            init.setInt(1, realmId);
            init.setString(2, volumeId);
            init.executeUpdate();
        }
        Long cursor = readCheckpoint(connection, true, realmId, volumeId);
        if (cursor == null) throw new ProjectionConflict("Volume is already registered to another realm");
        return cursor;
    }

    // 只读预演复用这两个 SELECT 来核验原卷身份和归档边界；它不能调用会登记空区服的 checkpoint()。
    static Long readCheckpoint(Connection connection, boolean lock, int realmId, String volumeId) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement(lock ? CHECKPOINT + " FOR UPDATE" : CHECKPOINT)) {
            query.setInt(1, realmId);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) return null;
                if (!volumeId.equals(row.getString("volume_id")))
                    throw new ProjectionConflict("Projection volume mismatch for realm " + realmId);
                long cursor = row.getLong("last_applied_seq");
                // 只核对水位所指的归档行，使用主键查一次，无需遍历全区历史。避免有人分开
                // 恢复游标表和流水表时，误把不存在的尾项视为已投影，从而跳过这段历史。
                if (cursor > 0 && !row.getBoolean("has_tail"))
                    throw new ProjectionConflict("Missing archived projection tail at sequence " + cursor);
                return cursor;
            }
        }
    }

    /** 即使序号在连续游标之内，也须区分真正的重复消息与来自不同历史的同号消息。 */
    static void verifyReplay(Connection connection, int realmId, Entry entry) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                SELECT entry_hash FROM projection_event WHERE realm_id=? AND journal_seq=?
                  AND scope_id=? AND request_id=? AND digest=? AND fact_type=? AND schema_version=?
                  AND fact_json=CAST(? AS JSON) AND result_json=CAST(? AS JSON)
                """)) {
            query.setInt(1, realmId);
            query.setLong(2, entry.sequence());
            query.setString(3, entry.scope());
            query.setString(4, entry.requestId());
            query.setString(5, entry.digest());
            query.setString(6, entry.factType());
            query.setInt(7, entry.schemaVersion());
            query.setString(8, entry.factJson());
            query.setString(9, entry.resultJson());
            try (ResultSet row = query.executeQuery()) {
                if (!row.next() || !MessageDigest.isEqual(row.getBytes(1), fingerprint(entry)))
                    throw new ProjectionConflict("Conflicting replay at sequence " + entry.sequence());
            }
        }
    }

    private void writeSuffix(Connection connection, List<Entry> suffix) throws SQLException {
        var states = new LinkedHashMap<String, StateWrite>();
        try (PreparedStatement archive = connection.prepareStatement("""
                INSERT INTO projection_event (realm_id,journal_seq,schema_version,scope_id,request_id,digest,
                    fact_type,fact_json,result_json,entry_hash) VALUES (?,?,?,?,?,?,?,?,?,?)
                """)) {
            for (Entry entry : suffix) {
                archive.setInt(1, realmId);
                archive.setLong(2, entry.sequence());
                archive.setInt(3, entry.schemaVersion());
                archive.setString(4, entry.scope());
                archive.setString(5, entry.requestId());
                archive.setString(6, entry.digest());
                archive.setString(7, entry.factType());
                archive.setString(8, entry.factJson());
                archive.setString(9, entry.resultJson());
                archive.setBytes(10, fingerprint(entry));
                archive.addBatch();
                for (Change change : entry.changes()) fold(states, change, entry.sequence());
            }
            archive.executeBatch(); // 无对象变更的拒绝决定仍归档；后续序号依赖这条记录。
        }
        writeStates(connection, states);
    }

    private record StateWrite(String type, String id, long expected, long version, long sequence, String json) {}

    /** 保留每一条事实，但同一事务内外界只能看到最后的状态。先逐次核对版本链，再只写一次热点对象。 */
    private static void fold(Map<String, StateWrite> states, Change change, long sequence) throws ProjectionConflict {
        String key = change.key();
        int split = key.indexOf(':');
        if (split < 1 || split == key.length() - 1)
            throw new ProjectionConflict("Invalid object key at sequence " + sequence);
        String type = key.substring(0, split);
        if (!OBJECT_TYPES.contains(type))
            throw new ProjectionConflict("Unsupported object type at sequence " + sequence);
        StateWrite previous = states.get(key);
        if (previous != null && previous.version() != change.expectedVersion())
            throw new ProjectionConflict("Broken object version chain at sequence " + sequence);
        states.put(key, new StateWrite(type, key.substring(split + 1),
                previous == null ? change.expectedVersion() : previous.expected(),
                change.expectedVersion() + 1, sequence, change.json()));
    }

    /**
     * 写入最终快照中的绝对值，例如城内有多少兵。003 在这同一次 INSERT/UPDATE 上用触发器维护
     * 玩家、采集点的专用查询列。它们与归档和区服游标一同提交或回滚，无需为每种对象另开连接。
     */
    private void writeStates(Connection connection, Map<String, StateWrite> states) throws SQLException {
        try (PreparedStatement create = connection.prepareStatement("""
                     INSERT INTO projection_object (object_version,journal_seq,state_json,realm_id,object_type,object_id)
                     VALUES (?,?,?,?,?,?)
                     """);
             PreparedStatement update = connection.prepareStatement("""
                     UPDATE projection_object SET object_version=?,journal_seq=?,state_json=?
                     WHERE realm_id=? AND object_type=? AND object_id=? AND object_version=?
                     """)) {
            for (StateWrite state : states.values()) {
                boolean isNew = state.expected() == 0;
                PreparedStatement stmt = isNew ? create : update;
                stmt.setLong(1, state.version());
                stmt.setLong(2, state.sequence());
                stmt.setString(3, state.json());
                stmt.setInt(4, realmId);
                stmt.setString(5, state.type());
                stmt.setString(6, state.id());
                if (isNew) {
                    create.addBatch();
                } else {
                    // 快照是完整替换。CAS 防止投影基线损坏时用后版本静默掩盖漏写或额外写入。
                    update.setLong(7, state.expected());
                    if (update.executeUpdate() != 1)
                        throw new ProjectionConflict("Object version mismatch at sequence " + state.sequence());
                }
            }
            create.executeBatch(); // 创建必须是首次写入，主键冲突会回滚整个事务。
        }
    }

    /** v1 指纹固定字段顺序和 UTF-8 编码；内部 JSON 沿用原卷字符串，不受 MySQL JSON 格式化影响。 */
    private static byte[] fingerprint(Entry entry) {
        var changes = entry.changes().stream().map(c -> List.of(c.key(), c.expectedVersion(), c.json())).toList();
        String tuple = Json.write(List.of(entry.schemaVersion(), entry.sequence(), entry.scope(), entry.requestId(),
                entry.digest(), entry.factType(), entry.factJson(), entry.resultJson(), changes));
        try { return MessageDigest.getInstance("SHA-256").digest(tuple.getBytes(StandardCharsets.UTF_8)); }
        catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }

    private static boolean invalidData(SQLException error) {
        // 连接失效、锁等待与死锁可以重试。SQLState 22（例如 JSON 数字超出 MySQL 范围）和 23
        //（唯一键 / 外键冲突）是固定输入的确定性失败；自动重送相同日志仍会失败，应留在此批报警。
        // 驱动可能用 BatchUpdateException 包装实际错误，遍历异常链以覆盖批写与类型化异常两种情况。
        for (SQLException sql = error; sql != null; sql = sql.getNextException()) {
            String state = sql.getSQLState();
            if (sql instanceof SQLDataException || sql instanceof SQLIntegrityConstraintViolationException
                    || (state != null && (state.startsWith("22") || state.startsWith("23")))) return true;
        }
        return false;
    }
}
