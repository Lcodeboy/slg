package com.slg.uc.persistence;

import java.util.Map;

/** UC 开发默认连接本机独立的账号库，生产部署可显式设置地址及 TLS 参数。 */
public final class DatabaseEnvironment {
    private static final String LOCAL_URL = "jdbc:mysql://127.0.0.1:13306/slg_uc?sslMode=DISABLED"
            + "&allowPublicKeyRetrieval=true&connectionTimeZone=UTC&connectTimeout=5000&socketTimeout=15000";

    private DatabaseEnvironment() {}

    public static String jdbcUrl() { return jdbcUrl(System.getenv()); }
    public static String user() { return user(System.getenv()); }
    public static String password() { return password(System.getenv()); }

    static String jdbcUrl(Map<String, String> env) {
        String url = env.getOrDefault("SLG_UC_JDBC_URL", LOCAL_URL);
        if (!url.startsWith("jdbc:mysql://")) throw new IllegalArgumentException("SLG_UC_JDBC_URL must use MySQL JDBC");
        return url;
    }

    static String user(Map<String, String> env) {
        String user = env.getOrDefault("SLG_UC_DB_USER", env.getOrDefault("SLG_DB_USER", "slg_uc"));
        if (user.isBlank()) throw new IllegalArgumentException("SLG_UC_DB_USER or SLG_DB_USER must not be empty");
        return user;
    }

    /** 密码按原值传递。显式空密码属于配置错误，错误信息只记录变量名。 */
    static String password(Map<String, String> env) {
        String password = env.getOrDefault("SLG_UC_DB_PASSWORD", env.get("SLG_DB_PASSWORD"));
        if (password == null || password.isEmpty()) {
            throw new IllegalArgumentException("SLG_UC_DB_PASSWORD or SLG_DB_PASSWORD must be set");
        }
        return password;
    }
}
