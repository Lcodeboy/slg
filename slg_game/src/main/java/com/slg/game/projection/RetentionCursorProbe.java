package com.slg.game.projection;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.net.InetAddresses;
import com.slg.core.protocol.Json;
import com.slg.game.storage.JournalRetentionPreview.Consumer;
import com.slg.game.storage.RealmJournal.Entry;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** 在线只读预演的 MySQL 取证。GAME 活着、有消费进度、进度确实属于本卷，这三项需要分别核对。 */
public final class RetentionCursorProbe {
    private static final int TIMEOUT_MS = 3000;
    // 只读连接只继承传输、字符编码和 TLS 选项。任意连接钩子、自动建库或初始化 SQL
    // 都可能在第一条 SELECT 之前运行，不能靠事后调用 setReadOnly(true) 来阻止。
    private static final Set<String> SAFE_OPTIONS = Set.of(
            "sslmode", "usessl", "requiressl", "verifyservercertificate", "allowpublickeyretrieval",
            "serverrsapublickeyfile", "trustcertificatekeystoreurl", "trustcertificatekeystorepassword",
            "trustcertificatekeystoretype", "clientcertificatekeystoreurl", "clientcertificatekeystorepassword",
            "clientcertificatekeystoretype", "fallbacktosystemtruststore", "fallbacktosystemkeystore",
            "tlsversions", "tlsciphersuites", "connectiontimezone", "servertimezone", "characterencoding",
            "useunicode", "connecttimeout", "sockettimeout");
    private RetentionCursorProbe() {}

    /** 调用方须读取本次原卷检查的同一固定视图，不能为找回游标而推进次实例的观察位置。 */
    @FunctionalInterface
    public interface EntryReader {
        Entry read(long sequence) throws IOException;
    }

    /**
     * 只在消费线程健康、两端水位观测一致时交付可信游标 Jc。超时、关停、错卷、未登记区服、
     * Jc 不存在或与归档指纹不符，都返回阻断状态。查询不会建库、初始化游标、重放写事务或移动原卷。
     */
    public static Consumer observe(Map<String, String> env, int realmId, UUID volumeId, EntryReader entries, long high) {
        String enabled = env.getOrDefault("SLG_PROJECTION_ENABLED", "true");
        if ("false".equalsIgnoreCase(enabled)) return new Consumer(realmId, volumeId, null, "DISABLED");
        if (!"true".equalsIgnoreCase(enabled)) return new Consumer(realmId, volumeId, null, "CONFIG_INVALID");

        JsonNode health;
        try {
            health = readHealth(env);
        } catch (IOException | IllegalArgumentException | URISyntaxException unavailable) {
            return new Consumer(realmId, volumeId, null, "HEALTH_UNAVAILABLE");
        }
        if (health == null || !health.path("status").asText().equals("UP"))
            return new Consumer(realmId, volumeId, null, "GAME_UNAVAILABLE");
        if (!Long.valueOf(realmId).equals(sequence(health.path("realmId")))
                || !volumeId.toString().equals(health.path("volumeId").textValue()))
            return new Consumer(realmId, volumeId, null, "HEALTH_IDENTITY_MISMATCH");
        JsonNode projection = health.path("projection");
        Long applied = sequence(projection.path("lastAppliedSeq"));
        String state = projection.path("state").asText("");
        if (!"CAUGHT_UP".equals(state) && !"CATCHING_UP".equals(state)) {
            // 只传递已知状态码，避免输出服务端返回的自由文本或敏感字段。
            return new Consumer(realmId, volumeId, applied, switch (state) {
                case "RETRYING", "BLOCKED", "DISABLED", "STARTING", "STALE" -> state;
                default -> "HEALTH_UNKNOWN";
            });
        }
        Long gameHigh = sequence(health.path("journalSeq"));
        if (applied == null || gameHigh == null || applied > high || applied > gameHigh
                || ("CAUGHT_UP".equals(state) && !applied.equals(gameHigh)))
            return new Consumer(realmId, volumeId, applied, "CURSOR_OUT_OF_RANGE");

        try (var connection = readOnlyConnection(env)) {
            Long actual = MySqlProjectionStore.readCheckpoint(connection, false, realmId, volumeId.toString());
            if (actual == null) return new Consumer(realmId, volumeId, null, "SQL_CHECKPOINT_MISSING");
            // 业务可以在健康探针与 SQL 读快照之间继续提交。水位变化时重做预演，不把两个时刻拼在一起。
            if (!actual.equals(applied)) return new Consumer(realmId, volumeId, actual, "CURSOR_CHANGED");
            if (actual > 0) {
                Entry boundary = entries.read(actual);
                if (boundary == null || boundary.sequence() != actual)
                    return new Consumer(realmId, volumeId, actual, "ORIGINAL_BOUNDARY_MISSING");
                MySqlProjectionStore.verifyReplay(connection, realmId, boundary);
            }
            // 这两次 SELECT 在同一一致性读事务中完成。rollback 显式结束只读快照。
            connection.rollback();
            return new Consumer(realmId, volumeId, applied, state);
        } catch (ProjectionConflict inconsistent) {
            return new Consumer(realmId, volumeId, applied, "SQL_BOUNDARY_CONFLICT");
        } catch (IOException unreadable) {
            return new Consumer(realmId, volumeId, applied, "ORIGINAL_BOUNDARY_UNAVAILABLE");
        } catch (IllegalArgumentException unsupported) {
            return new Consumer(realmId, volumeId, applied, "CONFIG_INVALID");
        } catch (SQLException unavailable) {
            // SQL 驱动报错可能携带连接信息；诊断仅返回稳定状态码。
            return new Consumer(realmId, volumeId, applied, "SQL_UNAVAILABLE");
        }
    }

    private static Long sequence(JsonNode field) {
        return field.isIntegralNumber() && field.canConvertToLong() && field.longValue() >= 0 ? field.longValue() : null;
    }

    private static Connection readOnlyConnection(Map<String, String> env) throws SQLException {
        String url = boundedJdbcUrl(env.get("SLG_PROJECTION_JDBC_URL"));
        String password = env.get("SLG_PROJECTION_DB_PASSWORD");
        String user = env.getOrDefault("SLG_PROJECTION_DB_USER", "slg_projection");
        if (password == null || password.isEmpty() || user.isBlank())
            throw new IllegalArgumentException("Missing projection configuration");
        var credentials = new Properties();
        credentials.setProperty("user", user);
        credentials.setProperty("password", password);
        var connection = DriverManager.getConnection(url, credentials);
        try {
            connection.setReadOnly(true);
            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            connection.setAutoCommit(false);
            return connection;
        } catch (SQLException setup) {
            connection.close();
            throw setup;
        }
    }

    /** 预演可以拒绝特殊驱动配置，但不能在连接时自动建库、运行自定义 SQL 或加载用户拦截器。 */
    static String boundedJdbcUrl(String jdbc) {
        if (jdbc == null || !jdbc.startsWith("jdbc:mysql://")) throw new IllegalArgumentException("Expected MySQL URL");
        URI target = URI.create(jdbc.substring("jdbc:".length()));
        if (target.getHost() == null || target.getRawUserInfo() != null || target.getRawFragment() != null
                || target.getPort() == 0 || target.getPort() > 65535 || !target.getRawPath().matches("/[a-zA-Z0-9_-]+"))
            throw new IllegalArgumentException("Expected one MySQL host and schema");
        var options = new ArrayList<String>();
        if (target.getRawQuery() != null && !target.getRawQuery().isEmpty()) {
            for (String option : target.getRawQuery().split("&")) {
                String key = option.split("=", 2)[0].toLowerCase(Locale.ROOT);
                if (!SAFE_OPTIONS.contains(key)) throw new IllegalArgumentException("Unsupported inspection connection option");
                if (!key.equals("connecttimeout") && !key.equals("sockettimeout")) options.add(option);
            }
        }
        options.add("connectTimeout=" + TIMEOUT_MS);
        options.add("socketTimeout=" + TIMEOUT_MS);
        return "jdbc:mysql://" + target.getRawAuthority() + target.getRawPath() + "?" + String.join("&", options);
    }

    private static JsonNode readHealth(Map<String, String> env) throws IOException, URISyntaxException {
        String host = env.getOrDefault("SLG_BIND", "127.0.0.1");
        if (host.equalsIgnoreCase("localhost")) host = "127.0.0.1";
        if (host.startsWith("[") && host.endsWith("]")) host = host.substring(1, host.length() - 1);
        // 仅解析 IP 字面量；不依赖 DNS 的当前结果，也不允许重定向到另一个主机。
        var address = InetAddresses.forString(host);
        if (address.isAnyLocalAddress()) host = host.contains(":") ? "::1" : "127.0.0.1";
        else if (address.isLoopbackAddress()) host = address.getHostAddress();
        else throw new IllegalArgumentException("Expected loopback address");
        int port = Integer.parseInt(env.getOrDefault("SLG_GAME_PORT", "8082"));
        if (port < 1 || port > 65535) throw new IllegalArgumentException("Invalid local GAME port");
        var endpoint = new URI("http", null, host, port, "/health", null, null);
        var request = (HttpURLConnection) endpoint.toURL().openConnection(Proxy.NO_PROXY);
        request.setInstanceFollowRedirects(false);
        request.setConnectTimeout(TIMEOUT_MS);
        request.setReadTimeout(TIMEOUT_MS);
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MS);
        try {
            if (request.getResponseCode() != 200) throw new IOException("GAME unavailable");
            try (var body = request.getInputStream()) {
                var buffer = new ByteArrayOutputStream();
                byte[] chunk = new byte[4096];
                for (int n; (n = body.read(chunk)) != -1;) {
                    if (buffer.size() + n > 65_536 || System.nanoTime() - end >= 0)
                        throw new IOException("Invalid GAME health response");
                    buffer.write(chunk, 0, n);
                }
                return Json.MAPPER.readTree(buffer.toByteArray());
            }
        } finally { request.disconnect(); }
    }
}
