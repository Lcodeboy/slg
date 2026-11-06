package com.slg.uc;

import com.slg.uc.persistence.JdbcPool;
import com.slg.uc.persistence.JdbcSchema;
import com.slg.uc.persistence.PoolSettings;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.util.Optional;

/**
 * 账号持久化入口，持有独立连接池，只保存加盐密码派生值，不保存明文密码。
 * 所有 UC 实例共用同一个账号写库，InnoDB 的自增主键在并发注册和进程重启后继续统一发号。
 * 关闭仓储即关闭其连接池，不能继续用于新的账号请求。
 */
public final class AccountRepository implements AutoCloseable {
    private final JdbcPool pool;
    private final Clock clock;

    /** 使用默认 MySQL 连接池配置创建仓储，保留现有调用方的构造方式。 */
    public AccountRepository(String url, String user, String password, Clock clock) throws SQLException {
        this(url, user, password, clock, PoolSettings.defaults());
    }

    /**
     * 建池并初始化账号表；MySQL 表属性由共享建表器校验，初始化失败时释放已建立的池。
     * clock 用于生成秒级创建时间，不能为空；数据库连接信息不能记录到日志。
     *
     * @throws SQLException 连接或建表失败，外部错误不包含原始驱动文本
     */
    public AccountRepository(String url, String user, String password, Clock clock, PoolSettings settings) throws SQLException {
        this.clock = java.util.Objects.requireNonNull(clock, "Clock required");
        pool = JdbcPool.open("uc", url, user, password, settings);
        boolean supportedIdentity;
        try (Connection connection = connect()) {
            JdbcSchema.createTable(connection, """
                CREATE TABLE IF NOT EXISTS uc_account (
                    account_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    username VARCHAR(32) NOT NULL UNIQUE,
                    password_salt VARBINARY(32) NOT NULL,
                    password_hash VARBINARY(64) NOT NULL,
                    iterations INTEGER NOT NULL,
                    created_at BIGINT NOT NULL
                )
                """);
            supportedIdentity = hasNumericIdentity(connection);
        } catch (SQLException e) {
            pool.close();
            throw new SQLException("Cannot initialize account database", e.getSQLState(), e.getErrorCode());
        } catch (RuntimeException | Error e) {
            pool.close();
            throw e;
        }
        if (!supportedIdentity) {
            pool.close();
            throw new SQLException("UC requires uc_account.account_id BIGINT AUTO_INCREMENT PRIMARY KEY; "
                    + "use a fresh UC database or complete a coordinated identity migration before startup", "0A000");
        }
    }

    /**
     * IF NOT EXISTS 在旧 UUID 表上也会成功。启动前核对带符号 BIGINT、自增以及单列主键，
     * 避免新 UC 给原账号签发另一套身份。旧账号与 GAME 卷中的角色引用必须一起迁移。
     */
    private static boolean hasNumericIdentity(Connection connection) throws SQLException {
        try (var statement = connection.createStatement(); var rows = statement.executeQuery("""
                SELECT 1 FROM information_schema.COLUMNS c
                WHERE c.TABLE_SCHEMA=DATABASE() AND c.TABLE_NAME='uc_account' AND c.COLUMN_NAME='account_id'
                  AND c.COLUMN_TYPE='bigint' AND c.EXTRA='auto_increment' AND c.COLUMN_KEY='PRI'
                  AND (SELECT COUNT(*) FROM information_schema.STATISTICS s WHERE s.TABLE_SCHEMA=c.TABLE_SCHEMA
                       AND s.TABLE_NAME=c.TABLE_NAME AND s.INDEX_NAME='PRIMARY')=1
                """)) {
            return rows.next();
        }
    }

    /**
     * 由 MySQL 在单条自动提交 INSERT 内分配正整数账号 ID。回滚、重复用户名等场景允许跳号。
     * 使用同一条 INSERT 的 generated keys，避免多个 UC 并发注册时读到别人的 ID。
     * 用户名规范化和密码长度检查由上层负责；重复用户名抛出数据库约束异常，不返回已有账号。
     * 本方法没有请求收据，不能将重复注册视为携带相同返回值的幂等成功。
     *
     * @return 新账号标识
     * @throws SQLException 连接不可用、用户名冲突或写入失败
     */
    public long create(String username, String plainPassword) throws SQLException {
        byte[] salt = PasswordHasher.salt();
        // 密码派生先于借用连接，避免耗时计算占用有限的数据库连接。
        byte[] hash = PasswordHasher.hash(plainPassword, salt, PasswordHasher.ITERATIONS);
        try (Connection connection = connect(); var statement = connection.prepareStatement(
                "INSERT INTO uc_account(username,password_salt,password_hash,iterations,created_at) VALUES(?,?,?,?,?)",
                Statement.RETURN_GENERATED_KEYS)) {
            statement.setString(1, username);
            statement.setBytes(2, salt);
            statement.setBytes(3, hash);
            statement.setInt(4, PasswordHasher.ITERATIONS);
            statement.setLong(5, clock.instant().getEpochSecond());
            statement.executeUpdate();
            try (var keys = statement.getGeneratedKeys()) {
                // INSERT 可能已提交，取号失败时交给上层提示先登录，不再执行一次 INSERT。
                if (!keys.next()) throw new SQLException("No generated account ID returned");
                return positiveAccountId(keys.getLong(1));
            }
        }
    }

    /** 按已规范化的用户名读取认证材料；不存在时返回空，不在仓储层比较密码或签发会话。 */
    public Optional<Account> find(String username) throws SQLException {
        try (Connection connection = connect(); var statement = connection.prepareStatement(
                "SELECT account_id,password_salt,password_hash,iterations FROM uc_account WHERE username=?")) {
            statement.setString(1, username);
            try (var rows = statement.executeQuery()) {
                if (!rows.next()) return Optional.empty();
                return Optional.of(new Account(positiveAccountId(rows.getLong(1)), rows.getBytes(2), rows.getBytes(3), rows.getInt(4)));
            }
        }
    }

    private static long positiveAccountId(long id) throws SQLException {
        if (id <= 0) throw new SQLException("Invalid account ID in account database", "22003");
        return id;
    }

    /** 仓储不缓存连接，保证每次操作结束后可通过 try-with-resources 归还到池中。 */
    private Connection connect() throws SQLException { return pool.getConnection(); }

    /** 幂等关闭仓储所拥有的池；调用方应先停止向仓储分派请求。 */
    @Override public void close() { pool.close(); }

    /**
     * 只供服务端密码校验使用的认证材料，不应作为接口响应返回。
     * 数组字段没有防御性复制，使用方须按只读数据对待；toString 不输出认证材料。
     *
     * @param accountId 账号标识
     * @param salt 随机盐字节
     * @param hash PBKDF2 派生结果
     * @param iterations 创建该派生结果时采用的迭代次数，登录时应沿用此值
     */
    public record Account(long accountId, byte[] salt, byte[] hash, int iterations) {
        /** 避免日志或调试字符串意外携带密码校验材料。 */
        @Override public String toString() { return "Account[redacted]"; }
    }
}
