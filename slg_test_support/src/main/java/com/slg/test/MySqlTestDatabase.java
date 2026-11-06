package com.slg.test;

import org.testcontainers.mysql.MySQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * 只用于测试的 MySQL 隔离库工厂，每次 createUrl 创建全新 schema，重启测试可反复使用同一 URL。
 * 优先使用 verify-mysql.py 注入的隔离容器，未提供配置时自动启动单 JVM 共享的 MySQL 8.4 容器。
 * 不读取开发库配置，不跳过数据库测试，不提供兼容数据库回退；失败消息不回显地址或凭证。
 * 仅记录并删除自己成功创建的 schema，JVM 退出时清理；自建容器同时由 Testcontainers 管理回收。
 */
public final class MySqlTestDatabase {
    private static final Pattern URL = Pattern.compile("^(jdbc:mysql://[^/?#@]+/)([A-Za-z0-9_-]+)(\\?[^#]*)?$");
    private static final Map<String, String> SCHEMAS = new LinkedHashMap<>();
    private static Host host;
    private static boolean closed;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { closeAll(); }
            catch (RuntimeException ignored) { System.err.println("Could not confirm cleanup of isolated MySQL test schemas"); }
        }, "mysql-test-cleanup"));
    }

    private MySqlTestDatabase() { }

    /** 创建随机且不与其他测试共享的 schema；调用方须关闭自己的连接池，但不能删除开发数据库。 */
    public static synchronized String createUrl() {
        Host target = host();
        String schema = "slg_test_" + UUID.randomUUID().toString().replace("-", "");
        String url = replaceSchema(target.url, schema);
        try (Connection connection = connect(target); var statement = connection.createStatement()) {
            statement.setQueryTimeout(10);
            // 故意采用默认非二进制排序规则，让应用建表器的显式二进制配置继续受到测试。
            statement.executeUpdate("CREATE DATABASE `" + schema + "` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci");
            SCHEMAS.put(url, schema);
            return url;
        } catch (SQLException e) {
            throw failure("Cannot create isolated MySQL test schema; test credentials require CREATE/DROP DATABASE privileges");
        }
    }

    /** 返回测试账号；首次调用会初始化隔离容器，可在 @BeforeAll 中预热。 */
    public static synchronized String user() { return host().user; }

    /** 返回测试密码；不得写入日志或断言消息。 */
    public static synchronized String password() { return host().password; }

    /** 提前删除已关闭连接的测试库；拒绝未知 URL，避免误删调用方提供的任何原有库。 */
    public static synchronized void dropUrl(String url) {
        String schema = SCHEMAS.get(url);
        if (schema == null) throw new IllegalArgumentException("Only a schema created by this helper may be dropped");
        try (Connection connection = connect(host()); var statement = connection.createStatement()) {
            statement.setQueryTimeout(10);
            statement.executeUpdate("DROP DATABASE `" + schema + "`");
            SCHEMAS.remove(url);
        } catch (SQLException e) { throw failure("Cannot clean up isolated MySQL test schema"); }
    }

    /** 幂等释放本助手的全部 schema 和自建容器，必须在所有测试连接关闭后调用。 */
    public static synchronized void closeAll() {
        if (closed) return;
        closed = true;
        if (host == null) return;
        boolean failed = false;
        try {
            if (!SCHEMAS.isEmpty()) {
                try (Connection connection = connect(host); var statement = connection.createStatement()) {
                    statement.setQueryTimeout(10);
                    for (String schema : SCHEMAS.values()) {
                        try { statement.executeUpdate("DROP DATABASE IF EXISTS `" + schema + "`"); }
                        catch (SQLException e) { failed = true; }
                    }
                } catch (SQLException e) { failed = true; }
            }
        } finally {
            SCHEMAS.clear();
            if (host.container != null) {
                try { host.container.stop(); }
                catch (RuntimeException e) { failed = true; }
            }
        }
        if (failed) throw failure("Could not confirm cleanup of isolated MySQL test resources");
    }

    /** 只有三项测试环境变量全部缺失才自动建容器；部分配置错误必须直接失败。 */
    private static Host host() {
        if (closed) throw failure("MySQL test resources have already been closed");
        if (host != null) return host;
        Configuration configured = configuration(System.getenv());
        if (configured != null) {
            Host candidate = new Host(configured.url, configured.user, configured.password, null);
            validate(candidate);
            return host = candidate;
        }
        String password = UUID.randomUUID().toString();
        String image = System.getenv().getOrDefault("SLG_MYSQL_TEST_IMAGE", "mysql:8.4");
        if (!image.matches("mysql:(8\\.4(?:\\.[0-9]+)?)") && !image.matches("mysql@sha256:[a-f0-9]{64}"))
            throw failure("SLG_MYSQL_TEST_IMAGE must be an official MySQL 8.4 tag or a verified MySQL digest");
        MySQLContainer container = new MySQLContainer(image)
                .withDatabaseName("slg_test_admin").withUsername("root").withPassword(password)
                .withEnv("MYSQL_ROOT_HOST", "%")
                .withUrlParam("sslMode", "DISABLED").withUrlParam("allowPublicKeyRetrieval", "true")
                .withUrlParam("connectionTimeZone", "UTC").withUrlParam("connectTimeout", "5000")
                .withUrlParam("socketTimeout", "15000").withStartupTimeout(Duration.ofMinutes(2));
        try {
            container.start();
            Host candidate = new Host(container.getJdbcUrl(), "root", password, container);
            validate(candidate);
            return host = candidate;
        } catch (RuntimeException e) {
            try { container.stop(); } catch (RuntimeException ignored) { }
            throw failure("Cannot start MySQL 8.4 test container; provide Docker or the SLG_MYSQL_IT_UC_URL/USER/PASSWORD test environment");
        }
    }

    /** 只读校验配置，显式错误不会连接默认开发库；包内入口用于不依赖容器的配置测试。 */
    static Configuration configuration(Map<String, String> environment) {
        String url = environment.get("SLG_MYSQL_IT_UC_URL");
        String user = environment.get("SLG_MYSQL_IT_USER");
        String password = environment.get("SLG_MYSQL_IT_PASSWORD");
        if (url == null && user == null && password == null) return null;
        if (url == null || user == null || user.isBlank() || password == null || password.isEmpty())
            throw failure("SLG_MYSQL_IT_UC_URL, SLG_MYSQL_IT_USER and SLG_MYSQL_IT_PASSWORD must all be set for the isolated test server");
        replaceSchema(url, "slg_test_validation");
        return new Configuration(url, user, password);
    }

    /** 保留显式测试服务器与连接参数，只替换 schema，禁止在 URL 权限段中嵌入凭证。 */
    static String replaceSchema(String url, String schema) {
        var matcher = URL.matcher(url);
        if (!matcher.matches() || !schema.matches("slg_test_[a-z0-9]+"))
            throw failure("Test database URL must be a MySQL server URL with a schema and without embedded credentials");
        return matcher.group(1) + schema + (matcher.group(3) == null ? "" : matcher.group(3));
    }

    private static Connection connect(Host target) throws SQLException {
        return DriverManager.getConnection(target.url, target.user, target.password);
    }

    private static void validate(Host target) {
        try (Connection connection = connect(target)) {
            if (!"MySQL".equals(connection.getMetaData().getDatabaseProductName()))
                throw failure("Isolated test server must be MySQL");
        } catch (SQLException e) { throw failure("Cannot connect to the explicitly configured isolated MySQL test server"); }
    }

    private static IllegalStateException failure(String message) { return new IllegalStateException(message); }

    /** 凭证值不参与默认对象字符串，避免测试错误输出把秘密带到报告中。 */
    record Configuration(String url, String user, String password) {
        @Override public String toString() { return "MySqlTestConfiguration[redacted]"; }
    }

    private record Host(String url, String user, String password, MySQLContainer container) {
        @Override public String toString() { return "MySqlTestHost[redacted]"; }
    }
}
