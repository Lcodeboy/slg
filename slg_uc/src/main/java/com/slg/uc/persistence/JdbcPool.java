package com.slg.uc.persistence;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.Objects;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 持有并管理一个 HikariCP 数据源，可供多个请求线程借用连接。
 * 调用方必须关闭每次借出的 Connection，以归还连接；池本身只在服务退出时关闭。
 * 单个 JDBC 连接及其事务仍由借用线程独占，不应跨请求共享。
 */
public final class JdbcPool implements AutoCloseable {
    private static final AtomicLong IDS = new AtomicLong();
    private final HikariDataSource dataSource;

    private JdbcPool(HikariDataSource dataSource) { this.dataSource = dataSource; }

    /**
     * 创建连接池，并在成功借还一个连接后交付使用；初始化失败时释放已创建的池。
     * 默认连接使用自动提交和 READ_COMMITTED，业务事务自行关闭自动提交。
     *
     * @param name 仅用于池名称的安全标识，不应包含凭证；允许 1～40 位小写字母、数字或连字符
     * @param url MySQL JDBC 地址，不接受其他驱动或兼容模式
     * @param user 数据库账号，不能为空引用
     * @param password 非空数据库密码
     * @param settings 连接数量及获取超时；超时单位为毫秒，不限制业务 SQL 执行时间
     * @throws SQLException 连接不可用或驱动配置失败，异常不保留原始驱动消息和原因链
     */
    public static JdbcPool open(String name, String url, String user, String password, PoolSettings settings)
            throws SQLException {
        Objects.requireNonNull(settings, "Pool settings required");
        if (name == null || !name.matches("[a-z0-9-]{1,40}") || url == null || !url.startsWith("jdbc:mysql://")
                || user == null || user.isBlank() || password == null || password.isEmpty())
            throw new IllegalArgumentException("Invalid database configuration");
        HikariDataSource source = new HikariDataSource();
        try {
            source.setPoolName("slg-" + name + "-" + IDS.incrementAndGet());
            source.setJdbcUrl(url);
            source.setUsername(user);
            source.setPassword(password);
            source.setMaximumPoolSize(settings.maximumPoolSize());
            source.setMinimumIdle(settings.minimumIdle());
            source.setConnectionTimeout(settings.connectionTimeoutMillis());
            source.setValidationTimeout(settings.validationTimeoutMillis());
            source.setMaxLifetime(settings.maxLifetimeMillis());
            source.setKeepaliveTime(settings.keepaliveMillis());
            source.setAutoCommit(true);
            source.setTransactionIsolation("TRANSACTION_READ_COMMITTED");
            // 让池异步建连，再以正常借用路径等待一个可用连接，统一获取超时和失败释放行为。
            source.setInitializationFailTimeout(-1);
            source.addDataSourceProperty("connectTimeout", Long.toString(settings.connectionTimeoutMillis()));
            source.addDataSourceProperty("tcpKeepAlive", "true");
            // 使用驱动级缓存，避免池自己复制预编译语句；参数来自 HikariCP 官方 MySQL 配置建议。
            source.addDataSourceProperty("cachePrepStmts", "true");
            source.addDataSourceProperty("prepStmtCacheSize", "250");
            source.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
            source.addDataSourceProperty("useServerPrepStmts", "true");
            source.addDataSourceProperty("useLocalSessionState", "true");
            source.addDataSourceProperty("rewriteBatchedStatements", "true");
            source.addDataSourceProperty("cacheResultSetMetadata", "true");
            source.addDataSourceProperty("cacheServerConfiguration", "true");
            source.addDataSourceProperty("elideSetAutoCommits", "true");
            source.addDataSourceProperty("maintainTimeStats", "false");
            try (Connection ignored = source.getConnection()) { }
            return new JdbcPool(source);
        } catch (SQLException e) {
            source.close();
            throw safeFailure("Database pool initialization failed", e);
        } catch (RuntimeException e) {
            source.close();
            // 驱动异常可能包含地址、凭证或连接属性，不能将原始消息和原因链向外传播。
            throw new SQLException("Database pool configuration failed", "08001");
        } catch (Error e) {
            source.close();
            throw e;
        }
    }

    /**
     * 在配置的等待时间内借用连接；池耗尽、已关闭或不可用时抛出脱敏后的 SQLException。
     * 调用方应使用 try-with-resources 归还连接；HikariCP 会回滚未提交工作并复位连接状态。
     */
    public Connection getConnection() throws SQLException {
        try { return dataSource.getConnection(); }
        catch (SQLException e) { throw safeFailure("Database connection unavailable", e); }
    }

    /** 保留可用于错误分类的 SQLState、厂商错误码和瞬时连接异常类型，去除可能携带秘密的文本。 */
    private static SQLException safeFailure(String message, SQLException original) {
        if (original instanceof SQLTransientConnectionException)
            return new SQLTransientConnectionException(message, original.getSQLState(), original.getErrorCode());
        return new SQLException(message, original.getSQLState(), original.getErrorCode());
    }

    /** 返回池是否已经关闭，不代表数据库当前是否健康。 */
    public boolean isClosed() { return dataSource.isClosed(); }
    /** 从连接池内存计数读取诊断快照，不为健康探针再借用 SQL 连接。 */
    public Map<String, Integer> metrics() {
        var pool = dataSource.getHikariPoolMXBean();
        if (pool == null) return Map.of("active", 0, "idle", 0, "total", 0, "waiting", 0,
                "maximum", dataSource.getMaximumPoolSize());
        return Map.of("active", pool.getActiveConnections(), "idle", pool.getIdleConnections(),
                "total", pool.getTotalConnections(), "waiting", pool.getThreadsAwaitingConnection(),
                "maximum", dataSource.getMaximumPoolSize());
    }
    /** 幂等关闭池及其连接；调用方应先停止接收新工作，避免中断仍在使用连接的事务。 */
    @Override public void close() { dataSource.close(); }
}
