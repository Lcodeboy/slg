package com.slg.game.infrastructure.mysql.runtime;

import java.net.URI;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.ArrayList;

/**
 * 权威数据库启动配置。四种用途共用显式单主库地址，不能用复制/负载均衡 URL 把一致读路由到从库。
 * 连接行为采用白名单；凭据只从独立环境项取得，禁止 URL 内凭据、自建库、重连或用户拦截器。
 * 本类不连接数据库，错误及 toString 都不回显配置值。
 */
public final class AuthorityDatabaseSettings {
    private static final Map<String, String> URL_OPTIONS = Map.of("sslmode", "sslMode",
            "allowpublickeyretrieval", "allowPublicKeyRetrieval", "characterencoding", "characterEncoding",
            "connectiontimezone", "connectionTimeZone", "servertimezone", "serverTimezone",
            "useunicode", "useUnicode", "enabledtlsprotocols", "enabledTLSProtocols", "tlsversions", "tlsVersions");
    private final String jdbcUrl, user, password;
    private final int txPoolSize, readPoolSize, mapPoolSize, controlPoolSize;
    private final int acquireTimeoutMillis, connectTimeoutMillis, socketTimeoutMillis, statementTimeoutSeconds;

    private AuthorityDatabaseSettings(Map<String, String> env) {
        jdbcUrl = validateUrl(required(env, "SLG_GAME_JDBC_URL"));
        user = required(env, "SLG_GAME_DB_USER");
        password = required(env, "SLG_GAME_DB_PASSWORD");
        txPoolSize = integer(env, "SLG_GAME_DB_TX_POOL_SIZE", 4, 1, 64);
        readPoolSize = integer(env, "SLG_GAME_DB_READ_POOL_SIZE", 8, 2, 128);
        mapPoolSize = integer(env, "SLG_GAME_DB_MAP_POOL_SIZE", 2, 1, 32);
        // 健康观察不能独占续租通道；控制池最少两条，仍须由组合根限制并发观察次数。
        controlPoolSize = integer(env, "SLG_GAME_DB_CONTROL_POOL_SIZE", 2, 2, 16);
        acquireTimeoutMillis = integer(env, "SLG_GAME_DB_ACQUIRE_TIMEOUT_MS", 2000, 500, 30000);
        connectTimeoutMillis = integer(env, "SLG_GAME_DB_CONNECT_TIMEOUT_MS", 3000, 250, 30000);
        socketTimeoutMillis = integer(env, "SLG_GAME_DB_SOCKET_TIMEOUT_MS", 5000, 1000, 60000);
        statementTimeoutSeconds = integer(env, "SLG_GAME_DB_STATEMENT_TIMEOUT_SECONDS", 3, 1, 30);
        if (socketTimeoutMillis <= statementTimeoutSeconds * 1000)
            throw new IllegalArgumentException("Database socket timeout must exceed the statement timeout");
    }

    public static AuthorityDatabaseSettings fromEnvironment(Map<String, String> environment) {
        return new AuthorityDatabaseSettings(Map.copyOf(Objects.requireNonNull(environment)));
    }
    public int txPoolSize() { return txPoolSize; }
    public int readPoolSize() { return readPoolSize; }
    public int mapPoolSize() { return mapPoolSize; }
    public int controlPoolSize() { return controlPoolSize; }
    public int acquireTimeoutMillis() { return acquireTimeoutMillis; }
    public int connectTimeoutMillis() { return connectTimeoutMillis; }
    public int socketTimeoutMillis() { return socketTimeoutMillis; }
    public int statementTimeoutSeconds() { return statementTimeoutSeconds; }
    String jdbcUrl() { return jdbcUrl; }
    String user() { return user; }
    String password() { return password; }
    @Override public String toString() { return "AuthorityDatabaseSettings[connection and credentials redacted]"; }

    private static String required(Map<String, String> env, String key) {
        String value = env.get(key);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Missing " + key);
        return value;
    }
    private static int integer(Map<String, String> env, String key, int fallback, int min, int max) {
        String raw = env.get(key);
        if (raw == null) return fallback;
        try {
            int value = Integer.parseInt(raw);
            if (value >= min && value <= max) return value;
        } catch (NumberFormatException invalid) { /* 错误只标记配置项，不携带原值或异常链。 */ }
        throw new IllegalArgumentException("Invalid " + key + "; expected " + min + ".." + max);
    }
    private static String validateUrl(String value) {
        try {
            if (!value.startsWith("jdbc:mysql://") || value.length() > 4096) throw new IllegalArgumentException();
            var uri = URI.create(value.substring("jdbc:".length()));
            if (uri.getHost() == null || uri.getRawUserInfo() != null || uri.getRawFragment() != null
                    || uri.getPort() == 0 || uri.getPort() > 65535 || uri.getPort() < -1
                    || !uri.getRawPath().matches("/[A-Za-z0-9_]{1,64}")) throw new IllegalArgumentException();
            var seen = new HashSet<String>();
            var options = new ArrayList<String>();
            if (uri.getRawQuery() != null) for (String option : uri.getRawQuery().split("&", -1)) {
                String[] pair = option.split("=", 2);
                String name = pair[0].toLowerCase(Locale.ROOT);
                if (pair.length != 2 || pair[1].isBlank() || !URL_OPTIONS.containsKey(name) || !seen.add(name))
                    throw new IllegalArgumentException();
                // 验证接受大小写变体时也统一驱动属性拼写，避免 TLS 等选项被驱动忽略后静默用默认值。
                options.add(URL_OPTIONS.get(name) + "=" + pair[1]);
            }
            return "jdbc:mysql://" + uri.getRawAuthority() + uri.getRawPath()
                    + (options.isEmpty() ? "" : "?" + String.join("&", options));
        } catch (IllegalArgumentException invalid) {
            // URI/驱动异常可能原样包含 userinfo 或口令，不能把 cause 接到对外异常。
            throw new IllegalArgumentException("SLG_GAME_JDBC_URL requires one MySQL primary host, database and supported TLS/encoding options");
        }
    }
}
