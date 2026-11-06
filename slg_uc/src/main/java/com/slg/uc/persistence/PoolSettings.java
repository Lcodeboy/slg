package com.slg.uc.persistence;

import java.util.Map;

/**
 * 连接池不可变配置；时间均为毫秒，不代表 SQL 或事务执行超时。
 *
 * @param maximumPoolSize 单个池可持有的最大物理连接数，范围为 1～64
 * @param connectionTimeoutMillis 等待借用连接的毫秒数，范围为 250～60000
 * @param minimumIdle 空闲连接保有数量，范围为 0～maximumPoolSize
 * @param validationTimeoutMillis 校验连接的等待上限，至少 250 且不大于借用超时
 * @param maxLifetimeMillis 物理连接最长寿命，范围为 30000～86400000；应短于外部连接回收时间
 * @param keepaliveMillis 空闲连接保活间隔，0 表示关闭，否则至少 30000 且小于最长寿命
 */
public record PoolSettings(int maximumPoolSize, long connectionTimeoutMillis, int minimumIdle,
                           long validationTimeoutMillis, long maxLifetimeMillis, long keepaliveMillis) {
    /** 在建池前拒绝无效边界值，避免将错误配置交给后台建连线程。 */
    public PoolSettings {
        if (maximumPoolSize < 1 || maximumPoolSize > 64)
            throw new IllegalArgumentException("SLG_DB_POOL_SIZE must be between 1 and 64");
        if (connectionTimeoutMillis < 250 || connectionTimeoutMillis > 60_000)
            throw new IllegalArgumentException("SLG_DB_CONNECTION_TIMEOUT_MS must be between 250 and 60000");
        if (minimumIdle < 0 || minimumIdle > maximumPoolSize)
            throw new IllegalArgumentException("SLG_DB_MINIMUM_IDLE must be between 0 and SLG_DB_POOL_SIZE");
        if (validationTimeoutMillis < 250 || validationTimeoutMillis > connectionTimeoutMillis)
            throw new IllegalArgumentException("SLG_DB_VALIDATION_TIMEOUT_MS must be between 250 and SLG_DB_CONNECTION_TIMEOUT_MS");
        if (maxLifetimeMillis < 30_000 || maxLifetimeMillis > 86_400_000)
            throw new IllegalArgumentException("SLG_DB_MAX_LIFETIME_MS must be between 30000 and 86400000");
        if (keepaliveMillis != 0 && (keepaliveMillis < 30_000 || keepaliveMillis >= maxLifetimeMillis))
            throw new IllegalArgumentException("SLG_DB_KEEPALIVE_MS must be 0 or at least 30000 and below SLG_DB_MAX_LIFETIME_MS");
    }

    /** 保留两参调用方式；默认固定池大小、30 分钟回收、2 分钟保活。 */
    public PoolSettings(int maximumPoolSize, long connectionTimeoutMillis) {
        this(maximumPoolSize, connectionTimeoutMillis, maximumPoolSize,
                Math.max(250, Math.min(1_000, connectionTimeoutMillis / 2)), 1_800_000, 120_000);
    }

    /** 返回开发默认配置：每池最多 8 个连接，借用等待上限 5 秒。 */
    public static PoolSettings defaults() { return new PoolSettings(8, 5_000); }

    /** 读取数量、借用/校验超时、回收和保活配置；未设置才采用默认值。 */
    public static PoolSettings fromEnvironment() { return fromEnvironment(System.getenv()); }

    /** 可注入环境映射的解析入口；显式空值、非整数或越界值均视为配置错误。 */
    static PoolSettings fromEnvironment(Map<String, String> environment) {
        int size = (int) number(environment, "SLG_DB_POOL_SIZE", 8, 1, 64);
        long timeout = number(environment, "SLG_DB_CONNECTION_TIMEOUT_MS", 5_000, 250, 60_000);
        return new PoolSettings(size, timeout,
                (int) number(environment, "SLG_DB_MINIMUM_IDLE", size, 0, size),
                number(environment, "SLG_DB_VALIDATION_TIMEOUT_MS", Math.max(250, Math.min(1_000, timeout / 2)), 250, timeout),
                number(environment, "SLG_DB_MAX_LIFETIME_MS", 1_800_000, 30_000, 86_400_000),
                number(environment, "SLG_DB_KEEPALIVE_MS", 120_000, 0, 86_400_000));
    }

    /** 先按 long 解析并检查范围，防止池大小在转为 int 时溢出后落入有效区间。 */
    private static long number(Map<String, String> environment, String key, long fallback, long min, long max) {
        String raw = environment.get(key);
        if (raw == null) return fallback;
        try {
            long value = Long.parseLong(raw);
            if (value >= min && value <= max) return value;
        } catch (NumberFormatException ignored) { }
        // 环境变量可能误填为秘密，只报告变量名和合法范围，不回显输入值。
        throw new IllegalArgumentException(key + " must be between " + min + " and " + max);
    }
}
