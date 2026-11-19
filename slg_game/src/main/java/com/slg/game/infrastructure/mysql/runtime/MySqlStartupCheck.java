package com.slg.game.infrastructure.mysql.runtime;

import com.slg.game.application.port.StorageAccessException;
import com.slg.game.domain.persistence.RealmIdentity;
import com.slg.game.infrastructure.mysql.session.MySqlErrors;
import com.slg.game.infrastructure.mysql.session.MySqlSessions;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.*;
import javax.sql.DataSource;
import static com.slg.game.application.port.StorageAccessException.Code.*;

/**
 * 取得写权前的只读验库。缺表、错身份、错误索引/引擎或不连续尾部均拒绝启动，绝不建库、
 * 补表、改游标或执行迁移。结构检查要求部署期间没有并发 DDL；检查成功不授予写权。
 * 周期 probe 只读同一 RR 视图的控制行，不能以一次健康观察代替事务内的 epoch/租约校验。
 */
public final class MySqlStartupCheck {
    private static final int METADATA_ROWS = 4096;
    private static final int TAIL_LIMIT = 128;
    private final DataSource control;
    private final int statementTimeoutSeconds;

    public MySqlStartupCheck(DataSource control, int statementTimeoutSeconds) {
        this.control = Objects.requireNonNull(control);
        if (statementTimeoutSeconds <= 0) throw new IllegalArgumentException("Positive startup statement timeout required");
        this.statementTimeoutSeconds = statementTimeoutSeconds;
    }

    public void verify(RealmIdentity identity) throws StorageAccessException {
        Objects.requireNonNull(identity);
        read(connection -> {
            requirePrimary(connection);
            verifySchema(connection);
            var observed = metadata(connection, identity);
            verifyTail(connection, identity.realmId(), observed);
            return null;
        });
    }

    /** 供健康线程缓存的有界观察；不扫描目录/事件、不开启后台线程、不获取或续期写权。 */
    public Snapshot probe(RealmIdentity identity) throws StorageAccessException {
        Objects.requireNonNull(identity);
        return read(connection -> { requirePrimary(connection); return metadata(connection, identity); });
    }

    public record Snapshot(long highSequence, long prunedThrough, long epoch, UUID ownerInstance,
                           boolean ownerActive, long expiresAtMillis, long databaseNowMillis) { }

    private <T> T read(Read<T> action) throws StorageAccessException {
        try (var connection = control.getConnection()) {
            connection.setReadOnly(true);
            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            connection.setAutoCommit(false);
            T value = action.run(connection);
            connection.rollback(); // 只结束读取视图；没有写入，更不需要提交空事务。
            return value;
        } catch (StorageAccessException failure) {
            // close 失败可能把含地址的 SQLException 作为 suppressed 附到业务异常上，脱去该链。
            throw new StorageAccessException(failure.code(), failure.getMessage());
        } catch (SQLException | RuntimeException failure) { throw MySqlErrors.access(failure); }
    }

    private PreparedStatement statement(Connection connection, String sql) throws SQLException {
        var statement = connection.prepareStatement(sql);
        try { statement.setQueryTimeout(statementTimeoutSeconds); return statement; }
        catch (SQLException failure) { statement.close(); throw failure; }
    }

    private void requirePrimary(Connection connection) throws SQLException, StorageAccessException {
        try (var query = statement(connection, "SELECT DATABASE(), @@global.read_only, @@global.super_read_only");
             var row = query.executeQuery()) {
            if (!row.next() || row.getString(1) == null)
                throw failure(IDENTITY_MISMATCH, "Authority connection has no selected database");
            if (row.getBoolean(2) || row.getBoolean(3))
                throw failure(UNAVAILABLE, "Authority database must be the writable primary");
        }
    }

    private void verifySchema(Connection connection) throws SQLException, StorageAccessException {
        String where = " WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN (" + AuthoritySchema.SQL_TABLE_LIST + ")";
        var tables = new HashSet<String>();
        try (var query = statement(connection, "SELECT TABLE_NAME, ENGINE, TABLE_TYPE FROM information_schema.TABLES" + where);
             var rows = query.executeQuery()) {
            while (rows.next()) {
                String table = rows.getString(1); tables.add(table);
                if (!"InnoDB".equalsIgnoreCase(rows.getString(2)) || !"BASE TABLE".equals(rows.getString(3)))
                    throw failure(UNSUPPORTED_SCHEMA, "Authority table must use InnoDB: " + table);
            }
        }
        for (var table : AuthoritySchema.TABLES) if (!tables.contains(table.name()))
            throw failure(UNSUPPORTED_SCHEMA, "Missing authority table: " + table.name());

        var columns = new HashMap<String, Set<String>>();
        try (var query = statement(connection, "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, "
                + "CHARACTER_MAXIMUM_LENGTH, COLLATION_NAME, COLUMN_TYPE, EXTRA FROM information_schema.COLUMNS" + where);
             var rows = query.executeQuery()) {
            int count = 0;
            while (rows.next()) {
                if (++count > METADATA_ROWS) throw failure(UNSUPPORTED_SCHEMA, "Authority column metadata exceeds startup budget");
                String table = rows.getString(1), name = rows.getString(2);
                var expected = AuthoritySchema.BY_NAME.get(table).columns().get(name);
                // schema v1 不容许额外必填/生成列改变既有 INSERT 语义；结构演进必须升级明确契约。
                if (expected == null)
                    throw failure(UNSUPPORTED_SCHEMA, "Unexpected authority column in schema v1: " + table);
                long length = rows.getLong(5); Long measured = rows.wasNull() ? null : length;
                if (!expected.type().equalsIgnoreCase(rows.getString(3))
                        || expected.nullable() != "YES".equals(rows.getString(4))
                        || expected.length() != null && !expected.length().equals(measured)
                        || !Objects.equals(expected.collation(), rows.getString(6))
                        || rows.getString(7).toLowerCase(Locale.ROOT).contains("unsigned")
                        || !"".equals(rows.getString(8)))
                    throw failure(UNSUPPORTED_SCHEMA, "Incompatible authority column: " + table + "." + name);
                columns.computeIfAbsent(table, ignored -> new HashSet<>()).add(name);
            }
        }
        for (var table : AuthoritySchema.TABLES) if (!columns.getOrDefault(table.name(), Set.of()).containsAll(table.columns().keySet()))
            throw failure(UNSUPPORTED_SCHEMA, "Missing required authority columns: " + table.name());

        var indexes = new HashMap<String, List<IndexColumn>>();
        try (var query = statement(connection, "SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME, "
                + "SUB_PART, INDEX_TYPE, IS_VISIBLE, COLLATION FROM information_schema.STATISTICS" + where
                + " ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX"); var rows = query.executeQuery()) {
            int count = 0;
            while (rows.next()) {
                if (++count > METADATA_ROWS) throw failure(UNSUPPORTED_SCHEMA, "Authority index metadata exceeds startup budget");
                String table = rows.getString(1), name = rows.getString(2);
                var required = AuthoritySchema.BY_NAME.get(table).indexes().stream().anyMatch(index -> index.name().equals(name));
                if (!required) continue;
                if (rows.getObject(6) != null || !"BTREE".equals(rows.getString(7))
                        || !"YES".equals(rows.getString(8)) || !"A".equals(rows.getString(9)))
                    throw failure(UNSUPPORTED_SCHEMA, "Authority index requires full visible ascending columns: " + table + "." + name);
                indexes.computeIfAbsent(table + "." + name, ignored -> new ArrayList<>())
                        .add(new IndexColumn(rows.getInt(4), rows.getString(5), rows.getInt(3) == 0));
            }
        }
        for (var table : AuthoritySchema.TABLES) for (var expected : table.indexes()) {
            var actual = indexes.getOrDefault(table.name() + "." + expected.name(), List.of());
            if (actual.size() != expected.columns().size())
                throw failure(UNSUPPORTED_SCHEMA, "Missing or incompatible authority index: " + table.name() + "." + expected.name());
            for (int i = 0; i < actual.size(); i++) {
                var column = actual.get(i);
                if (column.position() != i + 1 || column.unique() != expected.unique() || !expected.columns().get(i).equals(column.name()))
                    throw failure(UNSUPPORTED_SCHEMA, "Incompatible authority index columns: " + table.name() + "." + expected.name());
            }
        }
    }

    private Snapshot metadata(Connection connection, RealmIdentity identity) throws SQLException, StorageAccessException {
        try (var query = statement(connection, "SELECT dataset_id, schema_version FROM realm_identity WHERE realm_id=?")) {
            query.setInt(1, identity.realmId());
            try (var row = query.executeQuery()) {
                if (!row.next()) throw failure(IDENTITY_MISMATCH, "Authority realm is not initialized");
                if (row.getInt(2) != MySqlSessions.SCHEMA_VERSION)
                    throw failure(UNSUPPORTED_SCHEMA, "Unsupported authority schema version");
                if (!identity.datasetId().equals(uuid(row.getBytes(1))))
                    throw failure(IDENTITY_MISMATCH, "Authority dataset identity mismatch");
            }
        }
        // 普通一致读让 owner/lease/cursor 来自同一视图；绝不混用 FOR UPDATE 的当前读与旧 RR 行。
        try (var query = statement(connection, "SELECT c.high_seq, c.pruned_through, o.writer_epoch, o.owner_instance, o.active, "
                + "l.writer_epoch, l.owner_instance, l.expires_at_millis, "
                + "CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3))*1000 AS SIGNED) "
                + "FROM realm_commit_cursor c JOIN realm_owner o ON o.realm_id=c.realm_id "
                + "JOIN realm_owner_lease l ON l.realm_id=c.realm_id WHERE c.realm_id=?")) {
            query.setInt(1, identity.realmId());
            try (var row = query.executeQuery()) {
                if (!row.next()) throw failure(INVALID_DATA, "Authority owner, lease or cursor is missing");
                long high = row.getLong(1), floor = row.getLong(2), epoch = row.getLong(3), leaseEpoch = row.getLong(6);
                byte[] instance = row.getBytes(4), leaseInstance = row.getBytes(7);
                int active = row.getInt(5); long expires = row.getLong(8), now = row.getLong(9);
                if (high < 0 || floor < 0 || floor > high || epoch < 0 || epoch != leaseEpoch
                        || !Arrays.equals(instance, leaseInstance) || (active != 0 && active != 1) || now <= 0
                        || active == 0 && (instance != null || expires != 0)
                        || active == 1 && (epoch == 0 || instance == null || expires <= 0))
                    throw failure(INVALID_DATA, "Authority owner, lease or cursor metadata is inconsistent");
                return new Snapshot(high, floor, epoch, instance == null ? null : uuid(instance), active == 1, expires, now);
            }
        }
    }

    private void verifyTail(Connection connection, int realm, Snapshot snapshot) throws SQLException, StorageAccessException {
        // 只验证边界和最后128条序号，不用 COUNT 全表拖住启动。更早的事件完整性由有界事件读取继续核验。
        try (var query = statement(connection, "SELECT journal_seq FROM game_journal WHERE realm_id=? ORDER BY journal_seq DESC LIMIT 128")) {
            query.setInt(1, realm);
            try (var rows = query.executeQuery()) {
                long expected = snapshot.highSequence(); int count = 0;
                while (rows.next()) {
                    long sequence = rows.getLong(1);
                    if (expected <= snapshot.prunedThrough() || sequence != expected)
                        throw failure(INVALID_DATA, "Authority journal tail differs from its committed cursor");
                    expected--; count++;
                }
                if (count < TAIL_LIMIT && expected != snapshot.prunedThrough())
                    throw failure(INVALID_DATA, "Authority retained journal tail has a gap");
            }
        }
        try (var query = statement(connection, "SELECT journal_seq FROM game_journal WHERE realm_id=? ORDER BY journal_seq ASC LIMIT 1")) {
            query.setInt(1, realm);
            try (var row = query.executeQuery()) {
                boolean present = row.next();
                if (snapshot.highSequence() == snapshot.prunedThrough()) {
                    if (present) throw failure(INVALID_DATA, "Pruned authority journal still contains rows");
                } else if (!present || row.getLong(1) != snapshot.prunedThrough() + 1)
                    throw failure(INVALID_DATA, "Authority retained journal prefix differs from its prune boundary");
            }
        }
    }

    private static UUID uuid(byte[] bytes) throws StorageAccessException {
        if (bytes == null || bytes.length != 16) throw failure(INVALID_DATA, "Invalid authority UUID encoding");
        var buffer = ByteBuffer.wrap(bytes); return new UUID(buffer.getLong(), buffer.getLong());
    }
    private static StorageAccessException failure(StorageAccessException.Code code, String message) {
        return new StorageAccessException(code, message);
    }
    private record IndexColumn(int position, String name, boolean unique) { }
    @FunctionalInterface private interface Read<T> { T run(Connection connection) throws SQLException, StorageAccessException; }
}
