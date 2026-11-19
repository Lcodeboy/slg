package com.slg.game.infrastructure.mysql.runtime;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.PrintWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Properties;
import java.util.Map;
import java.util.List;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * 四个独立有界池：提交、玩家/待办读取、地图读取、写权控制互不借用容量。
 * 所有连接仍指向同一主库；readOnly 只限制会话写入，绝不代表允许从复制库读取。
 * 组合根须先排空使用者再 close，本类不负责停止业务线程或推断未知事务是否回滚。
 */
public final class AuthorityDataSources implements AutoCloseable {
    private final HikariDataSource tx, read, map, control;

    private AuthorityDataSources(AuthorityDatabaseSettings settings) {
        var opened = new ArrayList<HikariDataSource>();
        try {
            tx = pool(settings, "authority-tx", settings.txPoolSize(), false); opened.add(tx);
            read = pool(settings, "authority-read", settings.readPoolSize(), true); opened.add(read);
            map = pool(settings, "authority-map", settings.mapPoolSize(), true); opened.add(map);
            control = pool(settings, "authority-control", settings.controlPoolSize(), false); opened.add(control);
        } catch (RuntimeException failure) {
            closeAll(opened.reversed());
            throw new IllegalStateException("Authority database pools could not be opened");
        }
    }
    public static AuthorityDataSources open(AuthorityDatabaseSettings settings) {
        return new AuthorityDataSources(Objects.requireNonNull(settings));
    }
    public DataSource tx() { return tx; }
    public DataSource read() { return read; }
    public DataSource map() { return map; }
    public DataSource control() { return control; }
    /** 只读池内计数，不获取连接、执行 SQL 或输出地址/账户配置。 */
    public Map<String, Object> metrics() {
        return Map.of("tx", metrics(tx), "read", metrics(read), "map", metrics(map), "control", metrics(control));
    }
    private static Map<String, Integer> metrics(HikariDataSource pool) {
        var status = pool.getHikariPoolMXBean();
        return Map.of("active", status == null ? 0 : status.getActiveConnections(),
                "idle", status == null ? 0 : status.getIdleConnections(),
                "total", status == null ? 0 : status.getTotalConnections(),
                "waiting", status == null ? 0 : status.getThreadsAwaitingConnection(), "max", pool.getMaximumPoolSize());
    }
    @Override public void close() {
        if (!closeAll(List.of(control, map, read, tx)))
            throw new IllegalStateException("Authority database pools did not all close cleanly");
    }
    private static boolean closeAll(List<HikariDataSource> pools) {
        boolean closed = true;
        for (var pool : pools) try { pool.close(); }
        catch (RuntimeException failure) { closed = false; }
        return closed;
    }

    private static HikariDataSource pool(AuthorityDatabaseSettings settings, String name, int size, boolean readOnly) {
        var config = new HikariConfig();
        config.setPoolName(name);
        // 不把 URL/凭据放进 HikariConfig 的调试转储；连接入口同时移除驱动错误中的敏感文本。
        config.setDataSource(new PrimaryConnections(settings));
        config.setMaximumPoolSize(size); config.setMinimumIdle(0);
        config.setConnectionTimeout(settings.acquireTimeoutMillis());
        config.setValidationTimeout(Math.min(1000, settings.acquireTimeoutMillis() - 250));
        config.setInitializationFailTimeout(settings.acquireTimeoutMillis());
        config.setReadOnly(readOnly); config.setAutoCommit(true);
        config.setTransactionIsolation("TRANSACTION_REPEATABLE_READ");
        // 拒绝截断/数值钳制；只设置每条新连接的会话模式，不修改服务器全局配置或已有数据。
        config.setConnectionInitSql("SET SESSION sql_mode='STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION,"
                + "ERROR_FOR_DIVISION_BY_ZERO,NO_ZERO_DATE,NO_ZERO_IN_DATE,ONLY_FULL_GROUP_BY'");
        config.setMaxLifetime(1_800_000); config.setIdleTimeout(60_000);
        return new HikariDataSource(config);
    }

    /** JDBC 驱动仅在基础设施内使用；不允许独立调用参数覆盖账号，也不开放驱动日志流。 */
    private static final class PrimaryConnections implements DataSource {
        private final AuthorityDatabaseSettings settings;
        PrimaryConnections(AuthorityDatabaseSettings settings) { this.settings = settings; }
        @Override public Connection getConnection() throws SQLException {
            var properties = new Properties();
            properties.setProperty("user", settings.user()); properties.setProperty("password", settings.password());
            properties.setProperty("connectTimeout", Integer.toString(settings.connectTimeoutMillis()));
            properties.setProperty("socketTimeout", Integer.toString(settings.socketTimeoutMillis()));
            properties.setProperty("autoReconnect", "false"); properties.setProperty("allowMultiQueries", "false");
            properties.setProperty("createDatabaseIfNotExist", "false"); properties.setProperty("useAffectedRows", "false");
            try { return DriverManager.getConnection(settings.jdbcUrl(), properties); }
            catch (SQLException | RuntimeException failure) {
                throw new SQLException("Authority database connection unavailable", "08001");
            }
        }
        @Override public Connection getConnection(String username, String password) throws SQLException {
            throw new SQLFeatureNotSupportedException("Authority connections use the configured identity");
        }
        @Override public PrintWriter getLogWriter() { return null; }
        @Override public void setLogWriter(PrintWriter writer) throws SQLException {
            if (writer != null) throw new SQLFeatureNotSupportedException("Driver log streams are disabled");
        }
        @Override public void setLoginTimeout(int seconds) throws SQLException {
            if (seconds < 0) throw new SQLException("Invalid login timeout");
            // DriverManager 的全局超时会影响其他服务；本连接使用独立 connectTimeout。
        }
        @Override public int getLoginTimeout() { return (settings.connectTimeoutMillis() + 999) / 1000; }
        @Override public Logger getParentLogger() { return Logger.getLogger("com.slg.game.infrastructure.mysql.runtime"); }
        @Override public <T> T unwrap(Class<T> type) throws SQLException {
            if (type.isInstance(this)) return type.cast(this);
            throw new SQLException("Unsupported authority data source wrapper");
        }
        @Override public boolean isWrapperFor(Class<?> type) { return type.isInstance(this); }
        @Override public String toString() { return "AuthorityPrimaryConnections[redacted]"; }
    }
}
