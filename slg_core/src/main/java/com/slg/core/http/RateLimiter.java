package com.slg.core.http;

import java.util.HashMap;
import java.util.Map;

/**
 * 进程内、有键数量上限的固定窗口限流器，所有访问通过同步方法串行保护。
 * 使用单调时钟计时，每个键的窗口从首次请求开始；重启会清空计数，多实例间不共享额度。
 * 按 IP 限流时，调用方应传入真实连接地址，不能直接信任客户端提供的转发头。
 */
public final class RateLimiter {
    private final Map<String, Bucket> buckets = new HashMap<>();
    private final int maxKeys;
    private final int limit;
    private final long windowNanos;

    /**
     * 创建限流器；调用方须提供正数配置，本构造器不额外校验。
     *
     * @param limit 每个键在一个窗口内允许的请求数
     * @param windowSeconds 窗口时长，单位为秒，内部转为纳秒
     * @param maxKeys 最多保留的键数，用于限制客户端制造新键造成的内存增长
     */
    public RateLimiter(int limit, long windowSeconds, int maxKeys) {
        this.limit = limit;
        this.windowNanos = windowSeconds * 1_000_000_000L;
        this.maxKeys = maxKeys;
    }

    /**
     * 记录本次尝试并判断是否放行，超出额度的尝试也计入已有窗口。
     * 达到键数上限时先清理过期键；仍无空间则拒绝新键，已有键继续按自己的窗口判断。
     */
    public synchronized boolean allow(String key) {
        long now = System.nanoTime();
        Bucket bucket = buckets.get(key);
        if (bucket == null) {
            if (buckets.size() >= maxKeys) buckets.entrySet().removeIf(e -> now - e.getValue().start >= windowNanos);
            if (buckets.size() >= maxKeys) return false;
            buckets.put(key, new Bucket(now, 1));
            return true;
        }
        if (now - bucket.start >= windowNanos) { bucket.start = now; bucket.count = 0; }
        return ++bucket.count <= limit;
    }

    /** 单个键的可变窗口状态，只允许在外层同步方法内访问。 */
    private static final class Bucket {
        /** 单调时钟的纳秒起点，不能解释为 Unix 时间或持久化时间。 */
        long start;
        /** 当前窗口内累计的请求尝试数，包含超额尝试。 */
        int count;
        Bucket(long start, int count) { this.start = start; this.count = count; }
    }
}
